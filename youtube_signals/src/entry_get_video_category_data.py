import os
from datetime import datetime

import pandas as pd
from constants import VIDEO_LIMIT
from youtube import get_video_data, get_most_popular_videos

def get_video_category_data(year, month, day, time, video_category_id):
    print(f"Video category is {video_category_id}")

    most_popular_videos = get_most_popular_videos(video_category_id=video_category_id)

    results = []
    for i, video in enumerate(most_popular_videos):
        print(f"Getting data for submission no. {i}")
        video_data = get_video_data(video=video)
        if video_data:
            results.append(video_data)
        if len(results) == VIDEO_LIMIT:
            break
   
    df = pd.DataFrame(results)
    print(f"Shape of the final dataframe is {df.shape}")

    if df.empty:
        print("DF is emplty, won't be writing to CSV file...")
    else:
        output_path = f"s3://social-signals-dev-data/youtube/year={year}/month={month}/day={day}/time={time}/{video_category_id}.csv"
        print(f"Writing output to {output_path}")
        df.to_csv(output_path, index=False)


def main():
    print("Getting Youtube video category data...")

    execution_date_str = pd.to_datetime(str(os.environ["execution_date"])).strftime(
        "%Y-%m-%d-%H-%M-%S"
    )
    given_date = datetime.strptime(execution_date_str, "%Y-%m-%d-%H-%M-%S")
    year = given_date.strftime("%Y")
    month = given_date.strftime("%m")
    day = given_date.strftime("%d")
    time = given_date.strftime("%H%M%S")

    config = eval(os.environ["config"])
    video_category = str(config["video_category"])

    get_video_category_data(year, month, day, time, video_category)
    print(
        f"Finished getting Youtube video category data for year {year}, month {month}, day={day}, and time={time}"
    )


if __name__ == "__main__":
    main()
