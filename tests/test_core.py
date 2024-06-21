from extractors.core import write_to_file
import pandas as pd
import os
import pytest


def test_write_to_file_json():
    write_to_file("test_path.json", pd.DataFrame(), filetype="json")
    assert os.path.isfile("test_path.json")
    os.remove("test_path.json")
    # test parquet
    write_to_file("test_path.parquet", pd.DataFrame(), filetype="parquet")
    assert os.path.isfile("test_path.parquet")
    os.remove("test_path.parquet")


def test_write_to_file_invalid_type():
    with pytest.raises(ValueError):
        write_to_file("test_path.unknown", pd.DataFrame(), "invalid")
