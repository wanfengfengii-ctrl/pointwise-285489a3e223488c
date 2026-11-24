"""Run this project's full unittest suite and the interval demo from the root.

The unittest suite keeps every original regression.  The interval demo runs the
same scenario the manifest's ``demo`` entry prints, so ``verify`` fails (non-zero
exit) when the walkthrough's business assertions drift.
"""
import contextlib
import io
import sys
import unittest

import demo_interval


class _IntervalDemoCase(unittest.TestCase):
    def test_interval_demo(self):
        # ``demo_interval.main`` asserts its own business checks and raises
        # ``SystemExit`` on drift; convert that into a test failure.  Its
        # scenario line is only for the demo entry, so silence it here.
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                demo_interval.main()
            except SystemExit as exc:
                self.fail(f'interval demo failed: {exc}')


def _add_demo_case(loader, suite):
    suite.addTest(loader.loadTestsFromTestCase(_IntervalDemoCase))
    return suite


def main():
    loader = unittest.defaultTestLoader
    suite = loader.discover('tests', top_level_dir='.')
    _add_demo_case(loader, suite)
    if suite.countTestCases():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print('No tests discovered in tests/', file=sys.stderr)
        result = unittest.TestResult()
    for status, failed_cases in (
        ('FAIL', (test for test, _ in result.failures)),
        ('ERROR', (test for test, _ in result.errors)),
        ('UNEXPECTED_SUCCESS', result.unexpectedSuccesses),
    ):
        for test in failed_cases:
            print(f'{status} {test.id()}')
    successful = result.testsRun > 0 and result.wasSuccessful()
    print(f'TEST SUMMARY total={result.testsRun} failures={len(result.failures)} '
          f'errors={len(result.errors)} skipped={len(result.skipped)} '
          f'expected_failures={len(result.expectedFailures)} '
          f'unexpected_successes={len(result.unexpectedSuccesses)} '
          f'result={"PASS" if successful else "FAIL"}')
    return 0 if successful else 1


if __name__ == '__main__':
    raise SystemExit(main())
