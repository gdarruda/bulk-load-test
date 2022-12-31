from fastapi import FastAPI
import pandas as pd


def load_data():
    return (
        pd.read_parquet('inline.parquet')
          .set_index('id_client'))

data = load_data()
app = FastAPI()

@app.get("/")
def read_root():
    return {}

@app.get("/clients/{id_client}")
def read_item(id_client: str):    
    try:
        return data.loc[id_client].to_dict()
    except KeyError:
        return {}
