from pathlib import Path

BASE_SQL_DIR = Path(__file__).parent / "sql"

def read_sql(name: str) -> str:
    path = BASE_SQL_DIR / f"{name}.sql"
    with open(path, "r") as f:
        return f.read()