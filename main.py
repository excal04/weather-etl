from argparse import ArgumentParser
from datetime import date, timedelta
from extractors.core import DateInterval
from extractors.error import InvalidDateError, ResourceDownError
import logging as log

log.basicConfig(level=log.DEBUG)


def main():
    # argparse accept arguments
    parser = ArgumentParser()
    parser.add_argument(
        "--from",
        help="from date in format YYYY-MM-DD",
        type=date.fromisoformat,
        dest="from_",
    )
    parser.add_argument(
        "--to",
        help="to date in format YYYY-MM-DD",
        type=date.fromisoformat,
        default=date.today(),
    )
    args = parser.parse_args()
    if not args.from_:
        args.from_ = args.to - timedelta(days=7)
    log.info(f"transforming data within range {args.from_} -> {args.to}")
    from extractors.weather import run_weather_extractors

    # todo: get from env
    api_key = "ADU8S67Ddy!d7f?"
    try:
        run_weather_extractors(DateInterval(args.from_, args.to), api_key)
    except InvalidDateError:
        parser.error("FROM date must be before TO date")
    except ResourceDownError:
        print("extractor cannot finish")
        raise


if __name__ == "__main__":
    import time

    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))


# todo:
# dcoumnetaion
# logging
