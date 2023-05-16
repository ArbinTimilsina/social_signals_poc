import os
from datetime import datetime

import pandas as pd
from reddit import get_submission_data, get_top_submissions

SUBMISSION_TIME_FILTER = "day"
SUBMISSION_LIMIT = 25
COMMENT_SORT = "top"
COMMENT_LIMIT = 15


def get_subreddit_data(year, month, day, subreddit):
    results = []
    print(f"Subreddit is {subreddit}")

    top_submissions = get_top_submissions(
        subreddit=subreddit,
        time_filter=SUBMISSION_TIME_FILTER,
        limit=SUBMISSION_LIMIT,
    )
    for i, submission in enumerate(top_submissions):
        print(f"Getting data for submission no. {i}")
        submission_data = get_submission_data(
            submission=submission,
            comment_sort=COMMENT_SORT,
            comment_limit=COMMENT_LIMIT,
        )
        if submission_data:
            results.append(submission_data)
    df = pd.DataFrame(results)
    print(f"Shape of the final dataframe is {df.shape}")

    output_path = (
        f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/{subreddit}.csv"
    )
    print(f"Writing output to {output_path}")

    df.to_csv(output_path, index=False)


def main():
    print("Getting Reddit data...")

    execution_date_str = pd.to_datetime(str(os.environ["execution_date"])).strftime(
        "%Y-%m-%d"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")

    config = eval(os.environ["config"])
    subreddit = str(config["subreddit"])

    get_subreddit_data(year, month, day, subreddit)
    print(f"Finished getting Reddit data for year {year}, month: {month} , day={day}")


if __name__ == "__main__":
    main()
