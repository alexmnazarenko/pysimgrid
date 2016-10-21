"""
Very quick&dirty solution to discover tests and then run them in separate processes.
Essentially, it emulates CMake/CTest approach.

Typical 'pythonic' approaches doesn't work well:
* nosetests still tries to execute multiple test cases in a single process
* multiprocessing is unable to intercept/redirect native stdout from C library

Zero flexibility, runs everything on a working copy for now.
Can be vastly improved if necessary.
"""

from __future__ import print_function

import os
import sys
import re
import unittest
import subprocess
import time
import argparse

DEFAULT_TIMEOUT = 10
POLL_INTERVAL = 0.01
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TESTS_PACKAGE = "test"
TESTS_ROOT = os.path.join(PROJECT_ROOT, TESTS_PACKAGE)
TEST_MODULE_REGEX = re.compile("^test.*\.py$")

if PROJECT_ROOT not in sys.path:
  sys.path.insert(0, PROJECT_ROOT)

def collect_tests():
  tests = []
  for root, _, files in os.walk(TESTS_ROOT):
    for f in files:
      if TEST_MODULE_REGEX.match(f):
        test_file = os.path.join(root, f)
        module_name, _ = os.path.splitext(os.path.relpath(test_file, PROJECT_ROOT))
        module_name = module_name.replace("/", ".")
        module = __import__(module_name)
        for namepart in module_name.split(".")[1:]:
          module = getattr(module, namepart)
        for test_class in unittest.findTestCases(module)._tests:
          for test_function in test_class._tests:
            test_full_name = ".".join([module_name, type(test_function).__name__, test_function._testMethodName])
            tests.append((test_file, module_name, test_full_name))
  return tests

def run_tests(test_list, show_output):
  any_failed = False
  reports = []
  for filename, module, test in test_list:
    print("* ", "Starting", test, "...")
    # Python 2.7 compatibility kludges:
    #  * use open(os.devnull,...) instead of subprocess.DEVNULL
    #  * use POpen.poll
    with open(os.devnull, "w") as null_output:
      if show_output:
        output_config = {channel: None for channel in ["stdout", "stderr"]}
      else:
        output_config = {channel: null_output for channel in ["stdout", "stderr"]}
      start_time = time.time()
      process = subprocess.Popen([sys.executable, "-B", "-m", "unittest", test], cwd=PROJECT_ROOT, **output_config)
      while (time.time() - start_time) < DEFAULT_TIMEOUT:
        retcode = process.poll()
        if retcode is not None:
          break
      retcode = process.poll()
      test_failed = False
      execution_time_str = "{:.2f}s".format(time.time() - start_time)
      if retcode is None:
        process.kill()
        test_failed = True
        execution_time_str = "TIMEOUT ({} seconds)".format(DEFAULT_TIMEOUT)
      else:
        test_failed = retcode != 0
    report = " ".join(["  ", "FAILED" if test_failed else "PASSED", execution_time_str, test])
    reports.append(report)
    if not show_output:
      print(report)
    any_failed = any_failed or test_failed
  if show_output:
    print()
    print("=" * 30)
    print("Summary:")
    for report in reports:
      print(report)
  if any_failed:
    print("Some tests failed, exiting with non-zero code")
  else:
    print("All tests passed!")
  return int(any_failed)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-N", "--collect-only", action="store_true", default=False, help="don't run tests, only collect the list")
  parser.add_argument("-V", "--verbose", action="store_true", default=False, help="show verbose output")
  parser.add_argument("tests_to_run", nargs="*", help="optional: run tests by (any of passed) regex")
  args = parser.parse_args()
  test_list = collect_tests()
  if args.tests_to_run:
    filtered = []
    regexes = [re.compile(expr) for expr in args.tests_to_run]
    for test_data in test_list:
      _, _, test = test_data
      if any([r.search(test) for r in regexes]):
        filtered.append(test_data)
    test_list = filtered
  if args.collect_only:
    print("Tests found:")
    for filename, module, test in test_list:
      print("  ", test)
    return 0
  return run_tests(test_list, args.verbose)

if __name__ == '__main__':
  sys.exit(main())
