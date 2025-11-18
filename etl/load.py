import pandas as pd
from sqlalchemy import text, Engine


def load_to_dw(df: pd.DataFrame, table_name: str, schema, etl_conn: Engine):
    full_table_name = f"{schema}.{table_name}"

    print(f"LOAD: Cargando {len(df)} filas en {full_table_name}...")

    try:
        with etl_conn.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE;"))
            connection.commit()

            df.to_sql(
                name=table_name,
                con=etl_conn,
                schema=schema,
                if_exists='append',
                index=False
            )
        print(f"LOAD: Carga de {full_table_name} completada.")
    except Exception as e:
        print(f"ERROR: Falló la carga de {full_table_name}. Error: {e}")
        raise
