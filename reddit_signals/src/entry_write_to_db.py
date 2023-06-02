import os
from datetime import datetime

import pandas as pd
from constants import (
    CATEGORIES,
    COMMENT_LIMIT,
    COMMENT_SORT,
    ENTITIES,
    NONE_FILLER,
    SCHEMA,
    TABLE_NAME,
)
from reddit import process_submission_data
from sqlalchemy import create_engine
from tools import get_engine


def filter_submission_data(
    year, month, day, time, df, submission_ids, entity, category, top_n=3
):
    print(f"Processing entity {entity} and category {category}")

    df = df[(df[entity] != NONE_FILLER) & (df["categories"] == category)]
    print(f"Shape of the df is {df.shape}")

    submission_data_list = []
    count = 0
    for _, row in df.iterrows():
        submission_id = row["submission_id"]
        submission_title = row["submission_title"]
        submission_data = process_submission_data(
            submission_id=submission_id,
            submission_title=submission_title,
            comment_sort=COMMENT_SORT,
            comment_limit=COMMENT_LIMIT,
        )
        comments_summary = submission_data["comments_summary"]
        if comments_summary == NONE_FILLER:
            continue
        if submission_id in submission_ids:
            continue
        submission_ids.append(submission_id)

        submission_data["bucket"] = entity.capitalize()
        submission_data["year"] = year
        submission_data["month"] = month
        submission_data["day"] = day
        submission_data["time"] = time
        submission_data["title"] = submission_title

        subreddit_name = row["subreddit_name"]
        submission_data[
            "source"
        ] = f"https://reddit.com/r/{subreddit_name}/{submission_id}"

        entities = row[entity]
        submission_data["tags"] = entities

        submission_data_list.append(submission_data)
        count += 1
        if count == top_n:
            break
    return submission_data_list


def get_data_and_write_to_db(year, month, day, time):
    input_path = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/time={time}/combined.csv"
    df = pd.read_csv(input_path)
    print(f"Shape of the combined df is {df.shape}")

    submission_ids = []
    for entity in ENTITIES:
        for category in CATEGORIES:
            submission_data_list = filter_submission_data(
                year,
                month,
                day,
                time,
                df,
                submission_ids,
                entity=entity,
                category=category,
            )

        for submission_data in submission_data_list:
            db_df = pd.DataFrame(data=[submission_data])
            print(f"Writing df of shape {db_df.shape} to the DB")

            engine = get_engine()
            connection = create_engine(engine, pool_pre_ping=True)

            db_df.to_sql(
                name=TABLE_NAME,
                con=connection,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
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
