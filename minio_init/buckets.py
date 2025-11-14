from minio import Minio
import time


time.sleep(5)

client = Minio(
    "minio:9000",
    access_key="rootuser",
    secret_key="rootpass123",
    secure=False
)

bucketBronze = "bronze"
bucketSilver = "silver"
bucketGold = "gold"

# Création des buckets
if not client.bucket_exists(bucketBronze):
    client.make_bucket(bucketBronze)
    print(f"Bucket créé : {bucketBronze}")

if not client.bucket_exists(bucketSilver):
    client.make_bucket(bucketSilver)
    print(f"Bucket créé : {bucketSilver}")

if not client.bucket_exists(bucketGold):
    client.make_bucket(bucketGold)
    print(f"Bucket créé : {bucketGold}")