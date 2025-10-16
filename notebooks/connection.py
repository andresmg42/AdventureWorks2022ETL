import yaml
from sqlalchemy import create_engine, inspect, text

def connect(schema):
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
    co_oltp = create_engine(url_co)
    etl_conn = create_engine(url_etl)

    inspector2=inspect(co_oltp)
    inspector = inspect(etl_conn)
    tnames = inspector.get_table_names()
    tnames2=inspector2.get_table_names(schema=schema)

    print(tnames2)
    if not tnames:
        with etl_conn.connect() as conn:  
            with open('../sqlscripts.yml', 'r') as f:
                sql = yaml.safe_load(f)
                for key, val in sql.items():
                    conn.execute(text(val))
            conn.commit()
    return co_oltp,etl_conn 