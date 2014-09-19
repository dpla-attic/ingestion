import sys
from datetime import datetime
from dplaingestion.utilities import iso_utc_with_tz
from time import sleep

def test_iso_utc_with_tz():
    """iso_utc_with_tz() should yield a UTC datetime as a string with timezone symbol Z"""
    utc_now = datetime.utcnow()
    utc_now_iso_string_with_tz = utc_now.isoformat() + "Z"
    assert utc_now_iso_string_with_tz == iso_utc_with_tz(utc_now)
    # The following was added because of the regression described in #7699.
    time1 = iso_utc_with_tz()
    sleep(1)
    time2 = iso_utc_with_tz() 
    assert time1 != time2

if __name__ == "__main__":
    raise SystemExit("Use nosetest")