from io import BytesIO
from pathlib import Path

import pandas as pd


def load_csv_path(path: Path, **read_kw) -> pd.DataFrame:
    return pd.read_csv(path, **read_kw)


def load_csv_bytes(data: bytes, **read_kw) -> pd.DataFrame:
    return pd.read_csv(BytesIO(data), **read_kw)
