import pandas as pd 
from sqlalchemy import create_engine

arrival_file_path = "/home/pratyush/bangalore_air_traffic/data/BLR_ARRIVAL.csv"
departure_file_path = "/home/pratyush/bangalore_air_traffic/data/BLR_DEPARTURE.csv"

try:
    arrival_df = pd.read_csv(arrival_file_path, encoding='utf-8')
except UnicodeDecodeError:
    arrival_df = pd.read_csv(arrival_file_path, encoding='latin1')

try:
    departure_df = pd.read_csv(departure_file_path, encoding='utf-8')
except UnicodeDecodeError:
    departure_df = pd.read_csv(departure_file_path, encoding='latin1')

db_url = 'postgresql://root:root@localhost:5432/blr_air_traffic'

engine = create_engine(db_url)

arrival_df.to_sql('arrival_data', engine, if_exists='replace', index=False)
departure_df.to_sql('departure_data', engine, if_exists='replace', index=False)

print("Upload complete")