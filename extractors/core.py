import datetime
import logging as log
import os
from dataclasses import dataclass

import pandas as pd


DATE_FMT = "%Y-%m-%d"


@dataclass
class DateInterval:
    from_date: datetime.date
    to_date: datetime.date

    def get_date_range(self) -> pd.DatetimeIndex:
        """Returns a pandas date range between from_date and to_date inclusive.

        Returns:
            pd.DatetimeIndex: date range
        """
        return pd.date_range(self.from_date, self.to_date)

    def __str__(self):
        return f"`{self.from_date.strftime(DATE_FMT)}` -> `{self.to_date.strftime(DATE_FMT)}`"


def write_to_file(path: str, df: pd.DataFrame, filetype: str = "json"):
    """Write a dataframe to file path.

    Args:
        path (str): Filepath on local filesystem
        df (pd.DataFrame): Dataframe to write
        filetype (str, optional): Output filetype parquet or json (default)

    Raises:
        ValueError: raised when provided filetype is not recognized
    """
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
