import urllib
import urllib.error
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from extractors.core import DateInterval
from extractors.error import ResourceDownError
from extractors.weather import (
    WeatherExtractionType,
    clean_columns,
    get_data_url,
    make_output_filepath,
    run_solar_extraction,
    run_weather_extractors,
    run_wind_extraction,
)


def test_clean_columns():
    data = {
        "Naive_Timestamp ": [1717977600000],
        " Variable": [991],
        "value": [31.4485644825],
        "Last Modified utc": [1717977600000],
    }
    df = pd.DataFrame(data)
    df = clean_columns(df)
    assert 4 == len(df.columns)
    assert "Naive_Timestamp " not in df.columns
    assert "last_modified_utc" in df.columns


def test_make_output_filepath():
    timespan = DateInterval(from_date=date(2024, 6, 10), to_date=date(2024, 6, 12))
    path = make_output_filepath(timespan, WeatherExtractionType.SOLAR, "parquet")
    assert path.startswith("./output/solar/year=2024/month=06/day=10")
    assert path.endswith("parquet")


def test_get_data_url():
    url = get_data_url(WeatherExtractionType.WIND, "2024-06-10", "apikey")
    assert (
        url == "http://localhost:8000/2024-06-10/renewables/windgen.csv?api_key=apikey"
    )


@patch("extractors.weather.pd.read_json")
def test_run_solar_extraction(mock_read_json):
    timespan = DateInterval(date(2024, 6, 20), date(2024, 6, 20))
    data = {
        "Naive_Timestamp ": [1717977600000],
        " Variable": [991],
        "value": [31.4485644825],
        "Last Modified utc": [1717977600000],
    }
    expected_df = pd.DataFrame(data)
    mock_read_json.return_value = expected_df
    result_df = run_solar_extraction(timespan, "apikey")
    assert result_df.equals(expected_df)
    assert mock_read_json.called


@patch("extractors.weather.pd.read_csv")
def test_run_wind_extraction(mock_read_csv):
    timespan = DateInterval(date(2024, 6, 20), date(2024, 6, 20))
    data = {
        "Naive_Timestamp ": [1717977600000],
        " Variable": [991],
        "value": [31.4485644825],
        "Last Modified utc": [1717977600000],
    }
    expected_df = pd.DataFrame(data)
    mock_read_csv.return_value = expected_df
    result_df = run_wind_extraction(timespan, "apikey")
    assert result_df.equals(expected_df)
    assert mock_read_csv.called


@patch("extractors.weather.time.sleep")
@patch("extractors.weather.pd.read_csv")
def test_run_wind_extraction_api_failure(mock_read_csv: Mock, mock_sleep: Mock):
    timespan = DateInterval(date(2024, 6, 20), date(2024, 6, 20))
    # simulate an HTTPError
    mock_read_csv.side_effect = urllib.error.HTTPError(
        "url", 429, "api is dead", Mock(), Mock()
    )
    with pytest.raises(ResourceDownError):
        run_wind_extraction(timespan, "apikey")

    assert mock_read_csv.called
    # check that we did retries
    assert mock_sleep.called


@patch("extractors.weather.write_to_file")
@patch("extractors.weather.pd.read_csv")
@patch("extractors.weather.pd.read_json")
def test_run_weather_extractors(mock_read_json, mock_read_csv, mock_write_to_file):
    timespan = DateInterval(date(2024, 6, 20), date(2024, 6, 20))
    data = {
        "Naive_Timestamp ": [1717977600000],
        " Variable": [991],
        "value": [31.4485644825],
        "Last Modified utc": [1717977600000],
    }
    expected_df = pd.DataFrame(data)
    mock_read_json.return_value = expected_df
    mock_read_csv.return_value = expected_df
    run_weather_extractors(timespan, "apikey")
    assert mock_read_json.called
    assert mock_read_csv.called
    assert mock_write_to_file.called
    assert mock_write_to_file.call_count == 2
