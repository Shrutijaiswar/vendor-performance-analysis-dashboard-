import os
import pandas as pd
from sqlalchemy import create_engine
import logging

os.makedirs('logs', exist_ok=True)


logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)


engine = create_engine('sqlite:///inventory.db')


def ingest_db_chunked(file_path, table_name, engine, chunksize=10000):
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        success_msg = f"Inserted '{table_name}' successfully."
        print(success_msg)
        logging.info(success_msg)
    except Exception as e:
        error_msg = f"Error inserting '{table_name}': {e}"
        print(error_msg)
        logging.error(error_msg)


data_dir = 'data'


for file in os.listdir(data_dir):
    if file.endswith('.csv'):
        file_path = os.path.join(data_dir, file)
        table_name = file[:-4]  # remove .csv extension

        processing_msg = f" Processing: {file}"
        print(f"\n{processing_msg}")
        logging.info(processing_msg)

        ingest_db_chunked(file_path, table_name, engine)