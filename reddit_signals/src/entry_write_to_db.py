import os
from datetime import datetime

import pandas as pd
from constants import COMMENT_LIMIT, COMMENT_SORT
from reddit import process_submission_data
from sqlalchemy import create_engine

from common_tools.constants import (
    CATEGORIES,
    NONE_FILLER,
    SCHEMA,
    SUB_CATEGORIES,
    TABLE_NAME,
)
from common_tools.db import get_engine

CONNECTION = create_engine(get_engine(), pool_pre_ping=True)


def filter_and_write_to_db(
    year, month, day, time, df, submission_ids, category, sub_category, top_n=3
):
    print(f"Processing category {category} and sub-category {sub_category}")

    df_category = df[
        (df[category] != NONE_FILLER) & (df["sub_category"] == sub_category)
    ]
    print(f"Shape of the df is {df_category.shape}")

    count = 0
    for _, row in df_category.iterrows():
        submission_id = row["submission_id"]
        if submission_id in submission_ids:
            print(f"Found already proccessed id {submission_id}; skipping...")
            continue

        submission_title = row["submission_title"]
        submission_data = process_submission_data(
            submission_id=submission_id,
            submission_title=submission_title,
            comment_sort=COMMENT_SORT,
            comment_limit=COMMENT_LIMIT,
        )

        comments_summary = submission_data["comments_summary"]
        if len(comments_summary) < 5:
            print(f"Didn't find summary {submission_id}; skipping...")
            continue

        submission_ids.append(submission_id)

        submission_data["year"] = year
        submission_data["month"] = month
        submission_data["day"] = day
        submission_data["time"] = time
        submission_data["title"] = submission_title
        submission_data["social_signals_rank"] = row["social_signals_rank"]

        subreddit_name = row["subreddit_name"]
        submission_data[
            "source"
        ] = f"https://reddit.com/r/{subreddit_name}/{submission_id}"

        submission_data["category"] = category
        submission_data["sub_category"] = sub_category
        submission_data["tags"] = row[category]

        db_df = pd.DataFrame(data=[submission_data])
        print(f"Writing item {count} out of {top_n} to the DB")

        db_df.to_sql(
            name=TABLE_NAME,
            con=CONNECTION,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
        )

        count += 1
        if count == top_n:
            break


def get_data_and_write_to_db(year, month, day, time):
    input_path = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/time={time}/combined.csv"
    df = pd.read_csv(input_path)
    df = df.sort_values(by=["social_signals_rank"], ascending=False)
    print(f"Shape of the combined df is {df.shape}")

    submission_ids = []
    for category in CATEGORIES:
        for sub_category in SUB_CATEGORIES:
            filter_and_write_to_db(
                year,
                month,
                day,
                time,
                df,
                submission_ids,
                category=category,
                sub_category=sub_category,
            )


def main():
    print("Writing Reddit Signals to DB...")

    execution_date_str = pd.to_datetime(str(os.environ["execution_date"])).strftime(
        "%Y-%m-%d-%H-%M-%S"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d-%H-%M-%S")
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")
    time = given_date.strftime("%H%M%S")

    get_data_and_write_to_db(year, month, day, time)
    print(
        f"Finished writing Reddit Signals to DB for year {year}, month {month}, day={day}, and time={time}"
    )


if __name__ == "__main__":
    main()
