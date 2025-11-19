import yaml
import os
import sys
from sqlalchemy import create_engine, inspect, text


def connect():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    config_file_path = os.path.join(script_dir, "config_fill.yml")

    with open(config_file_path, "r") as f:
        config = yaml.safe_load(f)
        config_co = config["CO_SA"]
        config_etl = config["ETL_PRO"]
        

    # Construct the database URL
    url_co = (
        f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
        f"{config_co['port']}/{config_co['dbname']}"
    )
    url_etl = (
        f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
        f"{config_etl['port']}/{config_etl['dbname']}"
    )


    # Create the SQLAlchemy Engine
    co_oltp = create_engine(url_co)
    etl_conn = create_engine(url_etl)
    etl_conn_or = None 


    return co_oltp, etl_conn, etl_conn_or
