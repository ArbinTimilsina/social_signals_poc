import os
from datetime import datetime

import numpy as np
import pandas as pd
import s3fs
from constants import COMMENT_WEIGHT, SUBMISSION_WEIGHT


def get_combined_data(year, month, day, time):
    s3 = s3fs.S3FileSystem(anon=False)

    files_location = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/time={time}/*.csv"
    files = s3.glob(files_location)
    print(f"Processing files {files}")

    dfs = []
    for file in files:
        try:
            df = pd.read_csv(f"s3://{file}")
        except Exception as e:
            print(f"Couldn't read file {file} as df due to exception\n{e}")
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    print(f"Shape of the final dataframe is {df.shape}")

    print("Calculating up vote rank")
    df["up_vote_rank"] = (
        df["submission_up_votes_count"]
        .div(df["submission_views_count"])
        .replace(np.inf, 0.0)
    )

    print("Calculating comment rank")
    df["comment_rank"] = (
        df["submission_comments_count"]
        .div(df["submission_views_count"])
        .replace(np.inf, 0.0)
    )

    print("Calculating Social Signals rank")
    df["social_signals_rank"] = (SUBMISSION_WEIGHT * df["up_vote_rank"]) + (
        COMMENT_WEIGHT * df["comment_rank"]
    )

    df = df.sort_values(by=["social_signals_rank"], ascending=False)
    output_path = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/time={time}/combined.csv"
    print(f"Writing output to {output_path}")

    df.to_csv(output_path, index=False)


def main():
    print("Getting combined data...")

    execution_date_str = pd.to_datetime(str(os.environ["execution_date"])).strftime(
        "%Y-%m-%d-%H-%M-%S"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d-%H-%M-%S")
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")
    time = given_date.strftime("%H%M%S")

    get_combined_data(year, month, day, time)
    print(
        f"Finished getting combined data for year {year}, month {month}, day={day}, and time={time}"
    )


if __name__ == "__main__":
    main()
