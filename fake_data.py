import uuid
import pandas as pd
import numpy as np
from itertools import chain

NUM_CLASSES = 30
NUM_CLIENTS = 2_000_000

def generate_inline_df(num_rows: int,
                       num_classes: int) -> pd.DataFrame:
    
    return pd.DataFrame({
        **{"id_client": [str(uuid.uuid4()) for _ in range(num_rows)]},
        **{f"class_{c}": np.random.uniform(size=num_rows)
           for c in range(num_classes)}
    })

def generate_row_df(num_rows: int,
                    num_classes: int) -> pd.DataFrame:

    return pd.DataFrame({
        "id_client": list(chain(*[[str(uuid.uuid4())]*num_classes 
                                   for _ in range(num_rows)])),
        "class_name": [f"class_{c}" for c in range(num_classes)] * num_rows,
        "predict_value": np.random.uniform(size=num_rows*num_classes)
    })

generate_row_df(NUM_CLIENTS, NUM_CLASSES).to_parquet("normalized.parquet")
generate_inline_df(NUM_CLIENTS, NUM_CLASSES).to_parquet("inline.parquet")
