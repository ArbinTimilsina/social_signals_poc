import os
from datetime import datetime

import pandas as pd
from constants import COMMENT_LIMIT, COMMENT_SORT, SCHEMA, TABLE_NAME
from reddit import NONE_FILLER, process_submission_data
from sqlalchemy import create_engine
from tools import get_engine


def get_submission_data(year, month, day, df, entity, top_n=3):
    print(f"Processing entity {entity}")
    df_entity = df[(df[entity] != NONE_FILLER) & (df["comments_summary"] != NONE_FILLER)] 
    print(f"Shape of the df is {df_entity.shape}")

    submission_data_list = []
    for _, row in df_entity.head(n=top_n).iterrows():
        submission_id = row["submission_id"]
        submission_title = row["submission_title"]
        submission_data = process_submission_data(
            submission_id=submission_id,
            submission_title=submission_title,
            comment_sort=COMMENT_SORT,
            comment_limit=COMMENT_LIMIT,
        )
        submission_data["bucket"] = entity.capitalize()
        submission_data["year"] = year
        submission_data["day"] = day
        submission_data["month"] = month
        submission_data["title"] = submission_title

        subreddit_name = row["subreddit_name"]
        submission_data["source"] = f"reddit.com/r/{subreddit_name}/{submission_id}"

        entities = row[entity]
        submission_data["tags"] = entities

        submission_data_list.append(submission_data)
    return submission_data_list


def get_data_and_write_to_db(year, month, day):
    input_path = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/combined.csv"
    df = pd.read_csv(input_path)
    print(f"Shape of the combined df is {df.shape}")

    entities = ["organization", "person", "location"]
    for entity in entities:        
        submission_data_list = get_submission_data(year, month, day, df, entity=entity)

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
        "%Y-%m-%d"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")

    get_data_and_write_to_db(year, month, day)
    print(
        f"Finished writing Reddit Signals to DB for year {year}, month {month}, and day={day}"
    )


if __name__ == "__main__":
    main()
