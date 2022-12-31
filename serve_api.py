from fastapi import FastAPI

import pandas as pd
import numpy as np


def build_dict():
    df =  (
        pd.read_parquet('normalized.parquet')
          .assign(id_class = lambda df : df.id_class.astype(np.int8),
                  predict_value = lambda df : df.id_class.astype(np.float16))
          .pivot(index='id_client',
                 columns='id_class')
    )

    df.columns = [f"categoria_{i}" for i in range(len(df.columns))]
    return df.to_dict('index')

dictionary = build_dict()
app = FastAPI()

@app.get("/")
def read_root():
    return {}

@app.get("/clients/{id_client}")
def read_item(id_client: str):
    return dictionary.get(id_client, {})
