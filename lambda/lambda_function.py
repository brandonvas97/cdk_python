import requests
import json
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def handler(event, context):
    #Calling an external API
    request = requests.get("https://jsonplaceholder.typicode.com/users")
    data = request.json()

    table = pa.Table.from_pylist(data)

    # Ruta S3
    date = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
    route = f"s3://lambda-storage-bucket-mps-group/results/result_{date}.parquet"

    # Escribir a S3
    fs = s3fs.S3FileSystem()
    with fs.open(route, 'wb') as f:
        pq.write_table(table, f)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Archivo guardado en {route}")
    }