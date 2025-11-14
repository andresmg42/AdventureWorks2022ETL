import pandas as pd
from sqlalchemy import Engine

def _read_table(co_oltp: Engine, schema: str, table_name: str) -> pd.DataFrame:
    """Función auxiliar genérica para leer cualquier tabla."""
    print(f"EXTRACT: Leyendo {table_name}")
    return pd.read_sql_table(table_name, co_oltp, schema=schema)