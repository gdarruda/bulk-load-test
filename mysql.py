from sqlalchemy import create_engine

USER="root"
DATABASE="sample"
PASSWORD="example"
HOST="localhost"

def get_engine():
    return create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}:3306/{DATABASE}')