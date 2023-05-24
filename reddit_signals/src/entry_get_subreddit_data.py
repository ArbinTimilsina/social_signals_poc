import os
from datetime import datetime

import pandas as pd
from constants import SUBMISSION_LIMIT, SUBMISSION_TIME_FILTER
from reddit import get_submission_data, get_top_submissions


def get_subreddit_data(year, month, day, time, subreddit_name):
    results = []
    print(f"Subreddit is {subreddit_name}")

    subreddit, top_submissions = get_top_submissions(
        subreddit_name=subreddit_name,
        time_filter=SUBMISSION_TIME_FILTER,
        limit=SUBMISSION_LIMIT,
    )
    for i, submission in enumerate(top_submissions):
        print(f"Getting data for submission no. {i}")
        submission_data = get_submission_data(
            subreddit=subreddit, submission=submission
        )
        if submission_data:
            results.append(submission_data)
    df = pd.DataFrame(results)
    print(f"Shape of the final dataframe is {df.shape}")

    output_path = f"s3://social-signals-dev-data/reddit/year={year}/month={month}/day={day}/time={time}/{subreddit}.csv"
    print(f"Writing output to {output_path}")

    df.to_csv(output_path, index=False)


def main():
    print("Getting subreddit data...")

    execution_date_str = pd.to_datetime(str(os.environ["execution_date"])).strftime(
        "%Y-%m-%d-%H-%M-%S"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d-%H-%M-%S")
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")
    time = given_date.strftime("%H%M%S")

    config = eval(os.environ["config"])
    subreddit = str(config["subreddit"])

    get_subreddit_data(year, month, day, time, subreddit)
    print(
        f"Finished getting subreddit data for year {year}, month {month}, day={day}, and time={time}"
    )


if __name__ == "__main__":
    main()
