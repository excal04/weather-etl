import enum
import logging as log
import time
import urllib
import urllib.error
import uuid
from typing import Callable

import pandas as pd

from .core import DateInterval, write_to_file
from .error import InvalidDateError, ResourceDownError

# weather API configuraiton
# retries are performed when API unresponsive using these configurations
WEATHER_API_ENDPOINT = "http://localhost:8000"
MAX_RETRIES = 3
RETRY_DELAY_SEC = 1

# extractor output is written to disk
OUTPUT_DIR = "./output"


def run_weather_extractors(timespan: DateInterval, api_key: str):
    """Extracts weather data for a particular timespan and writes output to file.

    Args:
        timespan (DateInterval): Timespan with date range
        api_key (str): API key to use to fetch data

    Raises:
        InvalidDateError: Raised when timespan is invalid
    """
    if timespan.to_date < timespan.from_date:
        raise InvalidDateError()

    log.info(f"extracting data for timespan: {timespan}")

    output_type = "parquet"
    # extract solar and then extract wind
    extractors = {
        WeatherExtractionType.SOLAR: run_solar_extraction,
        WeatherExtractionType.WIND: run_wind_extraction,
    }
    for extractor_type, extraction_fn in extractors.items():
        log.info(f"running extractor {extractor_type}")
        df = extraction_fn(timespan, api_key)

        df = clean_columns(df)

        log.info(f"extraction complete for extractor:{extractor_type}, writing to file")
        filepath = make_output_filepath(timespan, extractor_type, output_type)
        write_to_file(filepath, df, filetype=output_type)


def run_solar_extraction(timespan: DateInterval, api_key: str) -> pd.DataFrame:
    """Extracts solar data from weather api and returns them as a DataFrame.

    Args:
        timespan (DateInterval): Date range for which data is extracted
        api_key (str): API key to use to fetch data

    Returns:
        pd.DataFrame: Combined solar data across timespan specified
    """
    frames = []
    for date in timespan.get_date_range():
        url = get_data_url(
            WeatherExtractionType.SOLAR, date.strftime("%Y-%m-%d"), api_key
        )
        df = _get_df_from_url(
            url, pd.read_json, convert_dates=["Naive_Timestamp ", "Last Modified utc"]
        )
        frames.append(df)
    df = pd.concat(frames)
    return df.reset_index(drop=True)


def run_wind_extraction(timespan: DateInterval, api_key: str) -> pd.DataFrame:
    """Extracts wind data from weather api and returns them as a DataFrame.

    Args:
        timespan (DateInterval): Date range for which data is extracted
        api_key (str): API key to use to fetch data

    Returns:
        pd.DataFrame: Combined wind data across timespan specified
    """
    frames = []
    for date in timespan.get_date_range():
        url = get_data_url(
            WeatherExtractionType.WIND, date.strftime("%Y-%m-%d"), api_key
        )
        df = _get_df_from_url(url, pd.read_csv)
        frames.append(df)
    df = pd.concat(frames)
    return df.reset_index(drop=True)


class WeatherExtractionType(enum.Enum):
    SOLAR = "solar"
    WIND = "wind"


def get_data_url(
    extraction_type: WeatherExtractionType, date: str, api_key: str
) -> str:
    """Return appropriate API location of weather data type

    Args:
        extraction_type (WeatherExtractionType): Weather data type
        date (str): API request parameter date
        api_key (str): API key to use to fetch data

    Raises:
        NotImplementedError: raised when extraction_type is not recognized

    Returns:
        str: absolute http url to GET data from
    """
    paths = {
        WeatherExtractionType.SOLAR: "solargen.json",
        WeatherExtractionType.WIND: "windgen.csv",
    }
    path = paths.get(extraction_type)
    if not path:
        raise NotImplementedError(f"No registered endpoint for: {extraction_type}")
    return f"{WEATHER_API_ENDPOINT}/{date}/renewables/{path}?api_key={api_key}"


def make_output_filepath(
    timespan: DateInterval, extraction_type: WeatherExtractionType, filetype: str
) -> str:
    """Returns a local filepath where weather output data can be stored.

    Example path structure: solar/year=2024/month=06/day=18/datafile.parquet

    Args:
        timespan (DateInterval): Timespan that extraction ran on
        extraction_type (WeatherExtractionType): Type of extracted data
        filetype (str): Output file type: json,parquet

    Returns:
        str: Filepath on local filesystem
    """
    week_format = timespan.from_date.strftime("year=%Y/month=%m/day=%d")
    path_to_output = f"{OUTPUT_DIR}/{extraction_type.value}/{week_format}"
    filepath = f"{path_to_output}/data-{uuid.uuid4()}.{filetype}"
    return filepath


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames and validates columns of data frame returning a clean one.

    Args:
        df (pd.DataFrame): Dataframe with raw data from weather endpoints

    Returns:
        pd.DataFrame: Dataframe with standardized columns
    """
    # rename columns to cleaner names
    column_mapping = {
        "Naive_Timestamp ": "timestamp",
        " Variable": "variable",
        "Last Modified utc": "last_modified_utc",
    }

    df = df.rename(columns=column_mapping)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["last_modified_utc"] = pd.to_datetime(df["last_modified_utc"], utc=True)

    # make sure correct columns and correct types
    transformed_columns = {"last_modified_utc", "timestamp", "value", "variable"}
    assert set(df.columns) == transformed_columns
    assert isinstance(df.last_modified_utc.dtype, pd.DatetimeTZDtype)
    assert isinstance(df.timestamp.dtype, pd.DatetimeTZDtype)
    assert pd.api.types.is_float_dtype(df.value.dtype)
    assert pd.api.types.is_integer_dtype(df.variable.dtype)

    return df


def _get_df_from_url(url: str, df_reader_fn: Callable, **kwargs) -> pd.DataFrame:
    """Utility function to fetch data from url and do retries on failures."""
    # note: blocking io which can be optimized
    for _ in range(MAX_RETRIES):
        try:
            return df_reader_fn(url, **kwargs)
        except urllib.error.HTTPError as e:
            log.warning(f"failed to GET from url:{url}, error:`{e}`, retrying")
        time.sleep(RETRY_DELAY_SEC)

    raise ResourceDownError(f"url unresponsive after {MAX_RETRIES} tries: {url}")
