import os
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener la URI de MongoDB desde la variable de entorno
mongo_uri = os.getenv("CONNECTION_URI")

# Conexión a MongoDB
client = MongoClient(mongo_uri, server_api=ServerApi('1'))

# Leer el archivo CSV
csv_file_path = "Chocolate Sales.csv" 
df = pd.read_csv(csv_file_path)

# Convertir la columna 'Date' a tipo datetime
df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%y")
# Convertir la columna 'Amount' a tipo numérico
df["Amount"] = df["Amount"].replace(r"[\$,]", "", regex=True).astype(float)

# Convertir DataFrame a diccionarios (registros)
data_dict = df.to_dict(orient="records")

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("\nConexión exitosa a MongoDB!")

    # Crear base de datos y colección
    db = client["ChocolateDB"]
    collection = db["ChocolateSales"]

    # Insertar los registros en la colección
    result = collection.insert_many(data_dict)
    print(f"{len(result.inserted_ids)} documentos insertados correctamente en la colección 'ChocolateSales'.")

except Exception as e:
    print(e)