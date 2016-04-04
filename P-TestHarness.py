#!/usr/bin/env python
import re
import time
import datetime
import itertools
from multiprocessing import Process, Queue


import softconfigs
import vplex_components1
from cluster import Cluster

from vats.settings import load
from vats.util.poll import wait
from vats.testcase import program, TestCase
from vats.util.subprocess import check_output

# Dictionary that maps the failure string with the description
# and the actual failure testcase file name
descMap = {
    'be_port_fail': 'BACKEND PORT FAILURE',
    'dir_fail': 'DIRECTOR FAILURE',
    'dir_fail_test': 'DIRECTOR FAILURE',
    'wancom_fail': 'WANCOM FAILURE',
    'migr': 'MIGRATION',
    'softcfg': 'SETUP TEARDOWN',
    'fe_port_fail': 'FRONTEND PORT FAILURE'
}

TCMap = {
    'be_port_fail': 'BEPortFailureTC',
    'dir_fail': 'directorFailureTC',
    'wancom_fail': 'WancomFailureTC',
    'migr': 'MigrationWithFailureTC',
    'dir_fail_test': 'MigrdirectorFailureTC',
    'softcfg': 'SetupTeardownWithFailureTC'
}

# Dictionary that failure/operation string with the DB field name that stores
# the number of times the failure was invoked
FailCntField = {
    'dir_fail': 'Director_Failure_Count',
    'wancom_fail': 'Wancom_Failure_Count',
    'be_port_fail': 'BE_Port_Failure_Count',
    'fe_port_fail': 'FE_Port_Failure_Count'
}

CntField = {
    'migr': 'Migration_Count',
    'mirror': 'Mir_Att_Det_Count',
    'softcfg': 'Setup_Teardown_Count'
}


class AsimoTestHarness(TestCase):

    @classmethod
    def setUpClass(cls):
        # Objects
        cls.vplex = Cluster(cls.settings)
        cls.sms = vplex_components1.active_sms(cls.settings)
        cls.result_dict = vplex_components1.get_vplex_system_info()
        cls.softcfg = softconfigs.SoftConfiguration(cls.settings)

        # Class variables
        cls.host_luids = []

        cls.scenario = None
        cls.failure = None
        cls.iterations = None
        cls.test_desc = None

        cls.op_queue = None
        cls.op_process = None
        cls.op_result = False
        cls.failure_queue = None
        cls.failure_process = None
        cls.failure_result = False

        cls.failure_count = 0
        cls.ops_count = 0

        cls.logger.info("*** DONE WITH SETUP CLASS ***")

    @classmethod
    def tearDownClass(cls):
        cls.restore_vplex_system()

        if cls.scenario == 'softcfg':
            # cls.softcfg.cleanup()
            pass

    def setUp(self):
        self.result_dict['Start_Time'] = datetime.datetime.now()
        self.result_dict['Iterations'] = self.iterations

    def tearDown(self):
        pass

    @classmethod
    def restore_vplex_system(cls):
        # Bring up all the directors and ports
        cls.logger.info('Bringing up all the directors')
        cls.vplex.rest.cmd('director run')

        cls.logger.info('Enabling all ports')
        cls.vplex.rest.set_attribute('/engines/**/ports/*', 'enabled', 'True')
   
    def perform_clean_up(self,result):
        self.logger.info(result)
        # Run the health check on the system if the test has succeeded
        if result:
            # Wait for the systems to completely recover from the
            # failures before checking for health of the system
            time.sleep(60)
            if system_validation.system_health_check(self.rest,self.logger):
                self.result_dict['Result'] = 'PASS'
            else:
                self.logger.error("-------System Health Check Failed-------!")
                self.result_dict['Comments'] = "Test passed but Health Check after test Failed"
                self.result_dict['Result'] = "FAIL"
        else:
            self.result_dict['Result'] = "FAIL"
            self.result_dict['Comments'] = "Test Failed"

        # Collect Logs and restore the system if there was a failure
        if (self.result_dict['Result'] == 'FAIL'):
            self.collect_logs()
        else:
            self.logger.info("*********Test Succeeded*********")

        self.update_DB(self.result_dict)

        # Bring up all the components so that the system is ready for the next test
        self.restore_vplex_system()

    def get_tap_dir_for_test_run(self, execute_file):
        """
        Return the Tap File for the test run.
        The VATS execute log file contains the TAP log file directory location
        which is parsed and returned.
        """
        tapFile = check_output(
            'grep "Logs located at:" %s' %
            execute_file, shell=True)
        tapFile = tapFile.replace('Logs located at: ', '').strip()
        self.logger.info('TAP File: %s', tapFile)
        return tapFile

    def get_failure_count(self, tap_file):
        """
        Extract the Failure Count statistic being reported in the tap file and
        return the same
        """
        FailCount = 0
        FailStr = check_output(
            'grep "Failure Count =" %s | tail -1' %(tap_file), shell=True)

        FailList = re.findall(r'Failure Count = (\d+)', FailStr)
        if FailList:
            FailCount = int(FailList[0])
        self.logger.info('FailCount = %s', FailCount)
        return FailCount

    def run_failure_tc(self, test, queue):
        """Execute the given test and put the result on to the given queue."""
        tc_result = True
        FailCnt = 0

        try:
            # Look for the latest vats execute log file before the run
            latest_log_cmd = 'ls -lrt vats_logs/vats_execute_* | tail -1'
            latest_log = check_output(latest_log_cmd, shell=True)
            self.logger.info('Latest log file before run: %s', latest_log)

            # Do not run the tests that will cause site failure while running
            # other operations. Ensure test scenarios that will cause site
            # failures are tagged with `sitefailure`.
            vats_cmd = 'vats execute --failfast --no-track ../tests/%s.py'
            if self.scenario is None:
                # TODO: What to do with the `test_output`
                test_output = check_output(vats_cmd % TCMap[test], shell=True)
            else:
                vats_cmd += ' --exclude-tags sitefailure'
                test_output = check_output(vats_cmd % TCMap[test], shell=True)

            # Get the log file that corresponds to current test run, fail the
            # test if no new execution log file was found.
            current_log = check_output(latest_log_cmd, shell=True)
            if current_log == latest_log:
                raise Exception('Test was not executed, aborting tests')
            else:
                log_file = current_log.split()[-1]
                self.logger.info('Vats Execute Log File: %s', log_file)

            # Accumulate the Failure Count reported by every test iteration
            # The failure count will be reported by the test in the
            tap_dir = self.get_tap_dir_for_test_run(log_file)
            if tap_dir:
                tap_file = '{}/{}.tap'.format(tap_dir, TCMap[test])
                self.logger.info('Vats TAP Log File : %s', tap_file)

                FailCnt += self.get_failure_count(tap_file)
            else:
                self.logger.error('Unable to find VATS tap results file')
                tc_result = False

        except Exception as e:
            self.logger.error('Exception - %s', str(e))
            # TODO: If there is an assertion error then fail the test
            tc_result = False

        self.logger.info('Result of the Failure Test run is - %s', tc_result)
        queue.put({'result': tc_result, 'fail_count': FailCnt})

    def setup_teardown(self, queue):
        """Start the soft config creation and tear down."""
        softcfg_status = True
        
        try:
            self.logger.info("Provisioning virtual volumes on VPlex")
            self.softcfg.provision_volumes(geometries=['r0'], count=1)
            self.logger.info("Provisioning completed, starting tear down")
            self.softcfg.teardown_volumes()
            self.logger.info("Tear down completed")
        except Exception as e:
            self.logger.error("=== Setup and Tear Down Failed ===")
            self.logger.error(e)
            softcfg_status = False
       
        queue.put({'result': softcfg_status})
        self.logger.info('Soft Configuration Status: %s', softcfg_status)
        self.logger.info('=== Setup and Tear Down Completed ===')

    def start_scenario(self, restart=False):
        """Start or restart the scenario specified in `self.scenario`."""
        self.op_queue = Queue()

        if self.scenario == 'softcfg':
            self.op_process = Process(target=self.setup_teardown,
                                      args=[self.op_queue])

            self.logger.info('Starting softcfg scenario')
            self.op_process.start()
        elif self.scenario == None:
            pass

    def start_failures(self):
        """Start the failure tests specified in `self.failure`."""
        self.failure_queue = Queue()

        self.failure_process = Process(target=self.run_failure_tc,
                                       args=[self.failure, self.failure_queue])

        self.logger.info('Starting %s failure test', self.failure)
        self.failure_process.start()


    def process_scenario_result(self, restart=True):
        """Assess the given scenario queue results and restart the
        process if necessary. If scenario process had failed, set
        the `self.failure_result` to False and terminate the failure test
        process. Else, if the `restart` is set to True
        then restart the soft configuration.
        """
        op_result = self.op_queue.get(timeout=30)
        self.logger.info('Scenario %s Results:', self.scenario)
        self.logger.info(op_result)

        # If operation has failed, simply return so that the failure
        # tests can finish/fail eventually.
        if op_result and op_result['result'] is True:
            self.op_result = True
            self.ops_count += 1

            msg = 'Iteration %s of %s has finished'
            self.logger.info(msg, self.ops_count, self.scenario)

            if restart:
                self.start_scenario(restart=restart)
        elif op_result['result'] is False:
            self.op_result = False
            self.logger.info('Scenario %s has failed', self.scenario)

    def process_failure_result(self):
        """Assess the failure test process result and take required
        actions. If the failure test is failed, then set `failure_result`
        to False and exit. Else, wait for the scenario to complete.
        """
        failure_result = self.failure_queue.get(timeout=30)
        self.logger.info('Test %s Results:', self.failure)
        self.logger.info(failure_result)

        if failure_result and failure_result['result'] is True:
            self.failure_result = True
            self.failure_count += failure_result['fail_count']
            self.logger.info('Test process has finished')
        elif failure_result['result'] is False:
            self.failure_result = False
            self.logger.error('Test %s has failed', self.failure)

        # self.logger.info('Waiting for scenarios to complete')
        # self.op_process.join()

    def wait_for_process(self):
        """Callback function to `wait` to determine if any one of the
        processes completed. Returns `True` if either `self.op_process`
        or `self.failure_process` completes.
        """
        if (self.failure_process.exitcode is None and
                self.op_process.exitcode is None):
            return False
        else:
            return True

    def start_tests(self):
        self.logger.info('Starting both failure and scenario processes')
        self.start_failures()
        self.start_scenario()

        self.logger.info('Waiting for at least one of the processes to complete')
        if not wait(self.wait_for_process, timeout=3600, sleep=1):
            self.failure_process.terminate()
            self.op_process.terminate()
            raise AssertionError('Processes failed to yield even after an hour.')

        self.logger.info('One or both processes completed, checking results')
        if self.failure_process.exitcode is not None:
            # If failure tests are completed, check the result of failure
            # tests and wait for scenario process to complete.

            if self.op_process.exitcode is None:
                self.logger.info('Waiting for scenarios to complete')
                self.op_process.join()

            self.process_failure_result()
            self.process_scenario_result(restart=False)
        elif self.op_process.exitcode is not None:
            # The scenario has completed, but failure tests might not have -
            # restart the scenarios till failure tests complete.
            while self.failure_process.exitcode is None:
                # Ensure scenario process has completed before attempting to
                # restart/create a new process.
                if self.op_process.exitcode is not None:
                    self.process_scenario_result(restart=True)
                    if self.op_result is False:
                        # Failure process has not completed but scenario
                        # process has failed. Wait till the failures tests
                        # are completed and break out of the for loop.
                        self.logger.info('Waiting for failure tests to complete')
                        self.failure_process.join()
                        break
                time.sleep(5)

            self.process_failure_result()
        else:
            raise Exception('No valid status code seen')

    def softcfg_with_failure(self):
        self.result_dict['Test_Operation'] = (self.scenario + '_with_' +
                                              self.failure)

        self.logger.info('### FAILURE TO BE RUN - %s ###', self.test_desc)

        self.failure_result = False
        self.op_result = False

        starttime = time.time()

        for iteration in range(1, self.iterations + 1):
            msg = '*** Running iteration %d of %s ***'
            self.logger.info(msg, iteration, self.test_desc)

            # Start the test and wait for at least one iteration of failure
            # test to complete.
            self.start_tests()

            # Verify if the tests were successful, else break out of the loop
            # so that we can gather required debug information and terminate
            # this test eventually.
            if self.failure_result and self.op_result:
                self.logger.info('Both scenario and failure tests have succeeded')
            else:
                self.logger.error('Error encountered, aborting further iterations')
                break

        self.result_dict['End_Time'] = datetime.datetime.now()
        test_duration = str(datetime.timedelta(seconds=(time.time() - starttime)))
        self.result_dict['Test_Duration'] = str(test_duration)
        self.result_dict[FailCntField[self.failure]] = self.failure_count
        self.result_dict['softcfg_count'] = self.ops_count

        # Run broader validation, cd (if the test has failed) and update the result to DB
        self.logger.info('*** Completed %d iterations of %s ***',
                         iteration, self.test_desc)

        # TODO: Need to implement this in a much cleaner way
        overall_result = self.failure_result and self.op_result
        self.result_dict['result'] = 'PASS' if overall_result else 'FAIL'

        msg = 'Test {} failed after {} iterations'
        self.assertTrue(overall_result is True,
                        msg.format(self.test_name, iteration))
        self.perform_clean_up(overall_result)


def get_config_file_from_args():
    """Parse the command line args and return VATS configuration file."""
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-c', '--config', dest='config_file',
                        help='Config file specifying vplex system info.')

    args, unknowns = parser.parse_known_args()
    return args


def load_tests_from_settings():
    """Load the config. file given in the args or the default config. file and
    and add test methods to the AsimoTestHarness class based on the arguments
    passed within the config. file (load default operation if nothing found).
    """
    args = get_config_file_from_args()
    config_file = args.config_file if args.config_file else ''
    print(config_file)

    settings = load(config_file)

    failures = settings.get('asimo', {}).get('failures', '')
    scenarios = settings.get('asimo', {}).get('scenarios', '')
    iterations = settings.get('asimo', {}).get('iterations', 1)
    # failures = settings['asimo']['failures']
    # scenarios = settings['asimo']['scenarios']
    # iterations = settings['asimo']['iterations']

    test_name = 'test_{}_with_{}'

    def get_test_method():
        def test_method(self):
            self.scenario = scenario
            self.failure = failure
            self.iterations = iterations
            self.test_desc = descMap[failure]

            if scenario is None:
                pass
            elif scenario == 'softcfg':
                self.softcfg_with_failure()

        return test_method

    failures = failures.split() if failures else ['wancom_fail']
    scenarios = scenarios.split() if scenarios else ['softcfg']
    # failures = failures.split() if failures else []
    # scenarios = scenarios.split() if scenarios else [None]
    for failure, scenario in itertools.product(failures, scenarios):
        tcname = test_name.format(scenario, failure)
        tcmethod = get_test_method()
        setattr(AsimoTestHarness, tcname, tcmethod)


if __name__ == '__main__':
    load_tests_from_settings()
    AsimoTestHarness.main()
