import os

import pandas as pd
import pytest

from extractors.core import write_to_file


def test_write_to_file_json():
    # test write json
    write_to_file("test_path.json", pd.DataFrame(), filetype="json")
    assert os.path.isfile("test_path.json")
    os.remove("test_path.json")
    # test write parquet
    write_to_file("test_path.parquet", pd.DataFrame(), filetype="parquet")
    assert os.path.isfile("test_path.parquet")
    os.remove("test_path.parquet")


def test_write_to_file_invalid_type():
    with pytest.raises(ValueError):
        write_to_file("test_path.invalid", pd.DataFrame(), "invalid")
