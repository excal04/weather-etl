import datetime
import os
from dataclasses import dataclass
import logging as log

import pandas as pd


@dataclass
class DateInterval:
    from_date: datetime.date
    to_date: datetime.date

    def get_date_range(self) -> pd.DatetimeIndex:
        return pd.date_range(self.from_date, self.to_date)


def write_to_file(path: str, df: pd.DataFrame, filetype: str = "json"):
    # create directories if not exist
    path_dir = os.path.dirname(path)
    if path_dir:
        os.makedirs(path_dir, exist_ok=True)

    log.info(f"writing {df.shape[0]} rows to {path}")
    if filetype == "json":
        return df.to_json(path)
    elif filetype == "parquet":
        return df.to_parquet(path)
    else:
        raise ValueError(f"Unsupported output format: `{filetype}`")
