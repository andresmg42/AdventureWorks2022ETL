import yaml
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

def connect():
    with open('../config_fill.yml', 'r') as f:
        config = yaml.safe_load(f)
        config_co = config['CO_SA']
        config_etl = config['ETL_PRO']

    # Construct the database URL
    url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
              f"{config_co['port']}/{config_co['dbname']}")
    url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
               f"{config_etl['port']}/{config_etl['dbname']}")

    # Create the SQLAlchemy Engine
    co_sa = create_engine(url_co)
    etl_conn = create_engine(url_etl)

    inspector_co = inspect(co_sa)
    schemas = inspector_co.get_schema_names()

    business_schemas = [s for s in schemas if s not in ['pg_catalog', 'information_schema', 'sysdiagrams', 'awbuild_version']]

    print("Esquemas encontrados:", business_schemas)

    all_tables = {}

    for schema_name in business_schemas:
        tables_in_schema = inspector_co.get_table_names(schema=schema_name)

        if tables_in_schema:
            all_tables[schema_name] = tables_in_schema

    # Imprimir los resultados
    print("\n--- Tablas de Negocio Encontradas ---")
    for schema, tables in all_tables.items():
        print(f"Esquema '{schema}' ({len(tables)} tablas):")
        # Imprime las primeras 5 tablas
        print(f"  > {tables[:5]}...")

    return inspector_co