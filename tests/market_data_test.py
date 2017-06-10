
from operations.market_data import get_market_db_file
import volsetup.config as config

def test_get_market_db_file():
    globalconf = config.GlobalConfig()
    db_type = "optchain_ib"
    expiry = "222222"
    print (get_market_db_file(globalconf, db_type, expiry))

if __name__ == "__main__":
    test_get_market_db_file()