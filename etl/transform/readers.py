import pandas as pd
from pathlib import Path

from sqlalchemy import Engine

BASE_SQL_DIR = Path(__file__).parent / "sql"

def read_sql(name: str) -> str:
    path = BASE_SQL_DIR / f"{name}.sql"
    with open(path, "r") as f:
        return f.read()

def load_dimension(name: str, etl_conn: Engine) -> pd.DataFrame:
    return pd.read_sql_table(name, etl_conn)