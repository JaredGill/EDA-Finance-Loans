import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect


def load_yaml():
    with open('credentials.yaml', 'r') as file:
        data = yaml.safe_load(file)
        print(data)
        return data


class RDSDatabaseConnector:

    def __init__(self, creds: dict) -> None:
        self.creds = creds

    def create_engine(self):
        #creds = RDSDatabaseConnector().load_yaml()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = self.creds['RDS_HOST'] 
        USER = self.creds['RDS_USER'] 
        PASSWORD = self.creds['RDS_PASSWORD']
        PORT = 5432
        DATABASE = self.creds['RDS_DATABASE'] 
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def extract_rds_table(self, engine, table_name: str) -> pd.DataFrame:
        table = pd.read_sql_table(table_name, engine)
        return table

    def save_df(self, df: pd.DataFrame, file_path: str):
        df.to_csv(file_path)

if __name__ == '__main__':
    cred_yaml = load_yaml()
    test = RDSDatabaseConnector(cred_yaml)
    engine = test.create_engine()
    engine.connect()
    list_db_tables = test.list_db_tables(engine)
    df = test.extract_rds_table(engine, 'loan_payments')
    print(df)
    file_path = '~/AiCore/EDA_Projects/Finance/loan_payments.csv'
    test.save_df(df, file_path)