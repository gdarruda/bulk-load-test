from sqlalchemy import create_engine

USER="postgres"
DATABASE="postgres"
PASSWORD="example"
HOST="localhost"

def get_engine():
    return create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:5432/{DATABASE}')