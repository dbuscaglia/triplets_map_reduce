try:
    import unittest2 as unittest
except ImportError:
    import unittest

from StringIO import StringIO
import string

from jobs.triplets_job import TripletFreqCount


class TripletTestCase(unittest.TestCase):

    def test_given(self):
        tough_string = """Hello, I like nuts. Do you like nuts? No? Are you sure? Why don't you like nuts? Are you nuts? I like you"""
        mr_job = self._get_job_from_input(tough_string)
        self._test_expected_for_job(self.standard_expected_dictionary(),
            mr_job)

    def test_oddly_formatted_file(self):
        """
        Make sure whitespace is allowed
        """
        tough_string = """Hello, I like nuts.
           Do you like nuts? No? Are you sure?
           Why don't you like nuts? Are you nuts?
           I like you"""
        mr_job = self._get_job_from_input(tough_string)
        self._test_expected_for_job(self.standard_expected_dictionary(),
            mr_job)

    def test_line_breaks(self):
        """
        This is an important test - makes sure our way of combining word
        pairs and throwing out pairs with only 1 work, independent of order
        """
        tough_string = "Hello, I like nuts.\nDo you like nuts?\n\
        No? Are you sure?\nWhy don't you like nuts?\n\
        Are you nuts?\nI like you"
        mr_job = self._get_job_from_input(tough_string)
        self._test_expected_for_job(self.standard_expected_dictionary(),
            mr_job)

    def standard_expected_dictionary(self):
        return {
            'are you' : 2,
            'like nuts': 3,
            'like you': 3,
            'i like': 2,
        }

    def test_simple_case(self):
        test_string = "This is\nA simple\nTest case"
        mr_job = self._get_job_from_input(test_string)
        expected_results = {}
        self._test_expected_for_job(expected_results, mr_job)

    def test_all_punctuation(self):
        for s in string.punctuation:
            test_string = "This is a Test case\nA simple not a simple\nTest{} case".format(s)
            mr_job = self._get_job_from_input(test_string)
            # Test case should never come
            expected_results = {
                "a simple": 2,
            }
            self._test_expected_for_job(expected_results, mr_job)

    def test_confusing_case(self):
        """
        The second line has A simple a simple, which should actually evaluate
        to a simple 3 times
        """
        test_string = "This is not\nA simple a simple\nTest case"
        mr_job = self._get_job_from_input(test_string)
        expected_results = {
            "a simple": 3,
        }
        self._test_expected_for_job(expected_results, mr_job)

    def _get_job_from_input(self, test_input):
        stdin = StringIO(test_input)
        mr_job = TripletFreqCount(['-r', 'inline', '--no-conf', '-'])
        mr_job.sandbox(stdin=stdin)
        return mr_job

    def _test_expected_for_job(self, expected_results, job):
        """
        Simple utility function to test results are as expected
        """
        results = {}
        with job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                # Use the job's specified protocol to read the output
                key, value = job.parse_output_line(line)
                results[key] = value
        self.assertDictEqual(expected_results, results)
