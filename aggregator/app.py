import os, io, time, json, random, string, datetime
import pandas as pd
from faker import Faker
from dateutil.relativedelta import relativedelta
from util import put_object_bytes

BRONZE_BUCKET = os.getenv("GOLD_BUCKET", "gold")

def aggregation():
    print("aggregation effectué")

def main():
    time.sleep(3)
    while True:
        try:
            aggregation()
        except Exception as e:
            print(f"[generator] ERROR: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()
