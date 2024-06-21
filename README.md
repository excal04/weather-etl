# weather-etl

Basic ETL application for weather data. This project is created for demonstration purposes only.

## Getting Started

1. This project runs in [python](https://www.python.org/downloads/), setup your python environment along with [pip](https://pip.pypa.io/en/stable/installation/) for package management. Install all the requirements.
    
    ```sh
    $ pip install -r requirements.txt
    ```

2. This project has 2 components - a web backend for data source, and a runnable application that demonstrates ETL. On one terminal run the web backend:

    ```sh
    $ python -m uvicorn api_data_source.main:app --reload
    ```

    More information on the api data source can be found in [README.api.md](README.api.md).

3. Once the web backend is running, on another terminal, run the ETL application.

    You have to export an environment variable with an api key: `WEATHER_API_KEY='ADU8S67Ddy!d7f?'`. This provides the ETL application access to the web api data source.

    ```sh
    $ WEATHER_API_KEY='ADU8S67Ddy!d7f?' python main.py 
    ```

    Command line options can be set on main.py, see `python main.py -h` for more information. By default it runs ETL for 1 week of data until the current day.

    ETL output is found in the `./output` directory.

## Development

The ETL application is developed using **Python** and **Pandas**. Everything is tested with **Python 3.9**.

### Setup

Development on a local environment is setup to work with poetry.

- [poetry](https://python-poetry.org/): package management and development workflow

The following are the common workflows that you'll need:
```sh
# installing the requirements
$ poetry install

# running the etl application
$ poetry run python main.py

# you can also drop into the virtualenv and omit poetry run in your commands
$ poetry shell
$ python main.py --from 2024-06-19 --to 2024-06-21
```

### Running Tests
The project uses `pytest` and `coverage` for unit tests and test coverage.

The following are the common workflows that you'll need:
```sh
# run the tests
$ poetry run pytest

# check test coverage
$ poetry run coverage run -m pytest
$ poetry run coverage report
```

## Data Documentation

### Data Source
Data source is taken from an API data source. This is documented in [README.api.md](README.api.md).

There are two primary data types for weather data: **solar** and **wind**. While they have identical data structures, the applicaiton maintains that these are separate data types.

Solar data structure is as follows:
```json
{
    "Naive_Timestamp ": 1717977600000,
    " Variable": 991,
    "value": 31.4485644825,
    "Last Modified utc": 1717977600000
}
```

Wind data structure:
```csv
Naive_Timestamp , Variable,value,Last Modified utc
2024-06-10 00:00:00+00:00,850,40.9958961662297,2024-06-10 00:00:00+00:00
```

### Data Transformation

The application will take the structure from above and do the following transformation
- convert `Naive_Timestamp ` and `Last Modified utc` to UTC timestamps.
- ` Variable` as int64
- `value` as float64
- clean the column names

The transformed data structure would produce:
```
timestamp            datetime64[ns, UTC]
variable                           int64
value                            float64
last_modified_utc    datetime64[ns, UTC]
```

An example or a row of data:
```
                timestamp  variable      value         last_modified_utc
2024-06-20 00:00:00+00:00       850  40.995896 2024-06-20 00:00:00+00:00
```