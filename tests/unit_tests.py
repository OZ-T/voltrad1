
from operations.market_data import get_market_db_file
import volsetup.config as config

# http://python-guide-pt-br.readthedocs.io/en/latest/writing/tests/

import unittest

def fun(x):
    return x + 1

class MyTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 4)

def test_get_market_db_file():
    globalconf = config.GlobalConfig()
    db_type = "optchain_ib"
    expiry = "222222"
    print (get_market_db_file(globalconf, db_type, expiry))

if __name__ == "__main__":
    test_get_market_db_file()