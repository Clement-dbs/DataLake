from util import get_object_bytes
from util import list_objects
import os,time, datetime
from util import put_object_bytes

# On stocke le dernier fichier traité
last_processed = None

# Vérifie si le fichier est nouveau
def check_if_new(data, last_data):
    return data != last_data

def get_bronze_customers_data():
    global last_processed

    list_data = list(list_objects("bronze", prefix="customers/")) # Je liste tous les fichiers dans le dossier customers du bucket bronze)
    last_data = sorted(list_data)[-1] # Récupère le dernier fichier basé de la liste

    if check_if_new(last_data, last_processed): # J'appelle check_if_new pour vérifier si le fichier est nouveau
        print(f"Nouveau fichier détecté : {last_data}")
        data = get_object_bytes("bronze", last_data)
        print(data)
        last_processed = last_data # On met a jour le dernier fichierr traité
        return data
    else:
        print(f"Aucun nouveau fichier")

    
def upload_to_silver_bucket(data):
    ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    put_object_bytes(SILVER_BUCKET, f"customers/customers_{ts}.csv", data, "text/csv")
    
def clean_data():
    data = get_bronze_customers_data() # Récupère le dernier fichier du bucket bronze/customers
    upload_to_silver_bucket(data) # On l'envoit dans le bucket silver/customers
    print("Nettoyage effectué !")

if __name__ == "__main__":
    BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")  
    SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver") 
    while True:
        try:
            clean_data()
        except Exception as exc:
            print("Aucune nouvelle donnée n'a été nettoyé", exc)
        time.sleep(5)
            





