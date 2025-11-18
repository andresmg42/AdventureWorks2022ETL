import pandas as pd
from dataclasses import dataclass, field
from sqlalchemy import Engine
from utils.model_loader import ModelRegistry


@dataclass
class ETLContext:
    oltp_engine: Engine
    dw_engine: Engine
    dest_schema: str
    model_registry: ModelRegistry
    dim_cache: dict[str, pd.DataFrame] = field(default_factory=dict)