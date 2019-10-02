import unittest
from NDATools import BuildPackage

class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, True)

    # process mocked list of csvs
    # see that skipvaliation skips local file check
    # process mocked manifest
    # process mocked s3 objects

if __name__ == '__main__':
    unittest.main()
