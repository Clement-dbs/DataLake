from util import get_object_bytes
import os,time, datetime
from util import put_object_bytes


def get_bronze_data():
    ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    print("récupération des données")
    return get_object_bytes(BRONZE_BUCKET, f"customers/customers_20251113T155037.csv")
    
def upload_to_silver_bucket():
    ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    csv_bytes = get_bronze_data()
    put_object_bytes(SILVER_BUCKET, f"customers/customers_{ts}.csv", csv_bytes, "text/csv")
    
def clean():
    upload_to_silver_bucket()
    print("Nettoyage effectué !")

if __name__ == "__main__":
    BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")  
    SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver") 
    while True:
        try:
            clean()
        except Exception as exc:
            print("error occurred.", exc)
            





