"""Skeleton for year-based bike rental and hourly weather ingestion."""

from __future__ import annotations

import argparse
from pathlib import Path
from io import BytesIO
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd


WEATHER_PATH = "s3://mlops-project-bucket-602343785232-ap-southeast-1-an/raw_data/open-meteo-38.91N77.07W12m.csv"

CATEGORIES = {
    "rideable_type": ["classic_bike", "docked_bike", "electric_bike"],
    "member_casual": ["casual", "member"],
}

S3_URL_TEMPLATE = (
    "https://s3.amazonaws.com/capitalbikeshare-data/"
    "{year:04d}{month:02d}-capitalbikeshare-tripdata.zip"
)



def build_monthly_url(year: int, month: int) -> str:
    """INPUT: A four-digit year and a month number from 1 through 12.

    RETURN: The source URL for that year's monthly ZIP archive.
    """
    return S3_URL_TEMPLATE.format(year=year, month=month)


def load_monthly_zip(
    year: int,
    month: int,
    cache_zip: bool = False,
    cache_path: Path = Path("ingestion/cache"),
) -> pd.DataFrame:
    """INPUT: A selected year and month.

    RETURN: The raw rental records extracted from the monthly ZIP archive.
    """

    # Build the source URL and stable local cache filename for this month.
    url = build_monthly_url(year, month)  # Source archive URL.
    cached_zip = cache_path / f"{year:04d}{month:02d}-capitalbikeshare-tripdata.zip"  # Local cache file.
    if cache_zip and cached_zip.exists():
        archive = BytesIO(cached_zip.read_bytes())
    else:
        with urlopen(url) as response:  # Download only when the cache is absent.
            archive_bytes = response.read()
        if cache_zip:
            cache_path.mkdir(parents=True, exist_ok=True)
            cached_zip.write_bytes(archive_bytes)
        archive = BytesIO(archive_bytes)
    with ZipFile(archive) as zip_file:
        csv_files = [name for name in zip_file.namelist() if name.lower().endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No CSV file found in {url}")
        with zip_file.open(csv_files[0]) as csv_file:
            return pd.read_csv(csv_file)


def aggregate_rental_counts(
    rentals: pd.DataFrame,
    categories: dict[str, list[str]],
) -> pd.DataFrame:
    """INPUT: Raw rental records containing ``started_at`` and configured
    category values.

    RETURN: One row per ``Year``/``Month``/``Day``/``Hour`` with one count
    column for each rideable-type and member-type combination.
    """
    # Convert trip start times into the hourly keys used by the final dataset.
    parsed_start = pd.to_datetime(rentals["started_at"], errors="coerce")
    enriched = rentals.assign(
        Year=parsed_start.dt.year,
        Month=parsed_start.dt.month,
        Day=parsed_start.dt.day,
        Hour=parsed_start.dt.hour,
    ).dropna(subset=["Year", "Month", "Day", "Hour"])

    # Count trips for each time point and ride/member category combination.
    grouped = (
        enriched.groupby(["Year", "Month", "Day", "Hour", "rideable_type", "member_casual"])
        .size()
        .rename("count")
        .reset_index()
    )
    # Build stable feature names such as classic_bike_member_count.
    grouped["count_column"] = (
        grouped["rideable_type"] + "_" + grouped["member_casual"] + "_count"
    )
    # Pivot category combinations into separate count columns; absent values become zero.
    counts = grouped.pivot_table(
        index=["Year", "Month", "Day", "Hour"],
        columns="count_column",
        values="count",
        fill_value=0,
    ).reset_index()
    # Ensure categories discovered in the reference month always exist in the output.
    expected_columns = [
        f"{ride_type}_{member_type}_count"
        for ride_type in categories["rideable_type"]
        for member_type in categories["member_casual"]
    ]
    for column in expected_columns:
        if column not in counts:
            counts[column] = 0

   

    result = counts[["Year", "Month", "Day", "Hour", *expected_columns]]
    # Verify duplicates without removing any rows during debugging.
    duplicate_rows = result[result.duplicated(
        subset=["Year", "Month", "Day", "Hour"], keep="first"
    )]
    print(f"Duplicate rows after first occurrence in aggregate: {len(duplicate_rows):,}")
    return result
 


def load_hourly_weather(year: int) -> pd.DataFrame:
    """INPUT: The selected year.

    RETURN: Hourly weather records with ``Year``, ``Month``, ``Day``, and
    ``Hour`` join columns and one temperature column.
    """
    weather = pd.read_csv(WEATHER_PATH, skiprows=3)
    required_columns = ["time", "temperature_2m (°C)"]
    missing_columns = [column for column in required_columns if column not in weather.columns]
    if missing_columns:
        raise ValueError(f"Weather file is missing required columns: {missing_columns}")

    parsed_time = pd.to_datetime(weather["time"], errors="coerce")
    weather = weather.assign(
        Year=parsed_time.dt.year,
        Month=parsed_time.dt.month,
        Day=parsed_time.dt.day,
        Hour=parsed_time.dt.hour,
    )
    weather = weather.loc[weather["Year"] == year].dropna(
        subset=["Year", "Month", "Day", "Hour"]
    )
    return weather[["Year", "Month", "Day", "Hour", "temperature_2m (°C)"]].rename(
        columns={"temperature_2m (°C)": "temperature"}
    )


def join_rentals_with_weather(
    rental_counts: pd.DataFrame,
    weather: pd.DataFrame,
) -> pd.DataFrame:
    """INPUT: Hourly rental counts and hourly weather records.

    RETURN: A DataFrame joined on ``Year``, ``Month``, ``Day``, and ``Hour``.
    """
    join_keys = ["Year", "Month", "Day", "Hour"]
    return rental_counts.merge(weather, on=join_keys, how="left", suffixes=("", "_weather"))




if __name__ == "__main__":



    # Parse the year range, month range, cache options, and output destinations.
    parser = argparse.ArgumentParser(description="Aggregate bike rental archives.")
    parser.add_argument("--start-year", type=int, required=True, help="First year to process")
    parser.add_argument("--end-year", type=int, required=True, help="Last year to process")
    parser.add_argument("--start-month", type=int, required=True, choices=range(1, 13), help="First month to process (1-12)")
    parser.add_argument("--end-month", type=int, required=True, choices=range(1, 13), help="Last month to process (1-12)")
    parser.add_argument("--cache-zip", action="store_true", help="Cache downloaded monthly ZIP files")
    parser.add_argument("--cache-path", type=Path, default=Path("ingestion/cache"), help="Local ZIP cache directory")
    parser.add_argument("--save-local", type=Path, help="Local output directory")
    parser.add_argument("--save-s3", help="S3 output directory URI")
    args = parser.parse_args()

    # Validate that the selected year and month windows are ordered.
    if args.start_year > args.end_year:
        parser.error("--start-year must be less than or equal to --end-year")
    if args.start_year == args.end_year and args.start_month > args.end_month:
        parser.error("For one year, --start-month must be less than or equal to --end-month")

    # Use the configured categories; discovery is intentionally skipped.
    categories = CATEGORIES
    hourly_frames = []

    # Process every month in the selected year/month window.
    for year in range(args.start_year, args.end_year + 1):
        first_month = args.start_month if year == args.start_year else 1
        last_month = args.end_month if year == args.end_year else 12
        for month in range(first_month, last_month + 1):
            try:
                monthly_df = load_monthly_zip(
                    year, month, cache_zip=args.cache_zip, cache_path=args.cache_path
                )
                hourly_frames.append(aggregate_rental_counts(monthly_df, categories))
                print(f"Aggregated {year}-{month:02d}")
            except (HTTPError, URLError, FileNotFoundError, ValueError) as error:
                print(f"Month {year}-{month:02d} unavailable; skipped: {error}")

    # Stop if none of the requested monthly archives could be loaded.
    if not hourly_frames:
        raise SystemExit("No requested monthly archives could be aggregated.")

    # Merge every monthly hourly frame before duplicate inspection and removal.
    final_df = pd.concat(hourly_frames, ignore_index=True)
    time_keys = ["Year", "Month", "Day", "Hour"]
    duplicate_rows = final_df[final_df.duplicated(subset=time_keys, keep="first")]
    print(f"Duplicate rows before removal: {len(duplicate_rows):,}")
    final_df = final_df.drop_duplicates(subset=time_keys, keep="first").reset_index(drop=True)
    print(f"Final DataFrame after duplicate removal: {len(final_df):,} rows")

    # Load weather once per selected year, then merge all weather frames.
    weather_frames = [
        load_hourly_weather(year)
        for year in range(args.start_year, args.end_year + 1)
    ]
    weather_df = pd.concat(weather_frames, ignore_index=True)
    final_df = join_rentals_with_weather(final_df, weather_df)
    print(f"Final DataFrame with weather: {len(final_df):,} rows and {len(final_df.columns)} columns")

    # Verify missing hourly time points across the selected date window.
    expected_start = pd.Timestamp(args.start_year, args.start_month, 1)
    expected_end = (pd.Timestamp(args.end_year, args.end_month, 1) + pd.offsets.MonthEnd(1)).replace(hour=23)
    expected_hours = pd.date_range(expected_start, expected_end, freq="h")
    actual_hours = pd.to_datetime(
        final_df[time_keys].rename(
            columns={"Year": "year", "Month": "month", "Day": "day", "Hour": "hour"}
        )
    )
    missing_time_points_df = expected_hours.difference(actual_hours.sort_values()).to_frame(
        index=False, name="missing_datetime"
    )
    print(f"Missing time points: {len(missing_time_points_df):,}")

    # Verify missing weather temperatures after the join.
    print(f"Time points missing weather temperature: {final_df['temperature'].isna().sum():,}")

    # Save the final DataFrame and missing-time report when destinations are supplied.
    output_filename = (
        f"bike_rental_y_{args.start_year}_{args.end_year}"
        f"_m_{args.start_month}_{args.end_month}.csv"
    )  # Include the selected year and month window in the output name.
    if args.save_local:
        args.save_local.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(args.save_local / output_filename, index=False)
        missing_filename = (
            f"time_point_missing_y_{args.start_year}_{args.end_year}"
            f"_m_{args.start_month}_{args.end_month}.csv"
        )  # Match the final output year/month naming window.
        missing_path = args.save_local / "missing" / missing_filename
        missing_path.parent.mkdir(parents=True, exist_ok=True)
        missing_time_points_df.to_csv(missing_path, index=False)
    if args.save_s3:
        s3_base = args.save_s3.rstrip("/")
        final_df.to_csv(f"{s3_base}/{output_filename}", index=False)
        missing_time_points_df.to_csv(
            f"{s3_base}/missing/time_point_missing_y_{args.start_year}_{args.end_year}"
            f"_m_{args.start_month}_{args.end_month}.csv",
            index=False,
        )
