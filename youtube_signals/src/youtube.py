import os
from datetime import datetime

from googleapiclient.discovery import build

from common_tools.common_constants import CLASSIFICATION_THRESHOLD, NONE_FILLER
from common_tools.sagemaker_inference import get_categories, get_emotion, get_ner
from common_tools.sumy_summary import get_sumy_summary

config = eval(os.environ["config"])
GOOGLE_API_KEY = config["google_api_key"]


def get_youtube():
    youtube = build("youtube", "v3", developerKey=GOOGLE_API_KEY)
    return youtube


def get_most_popular_videos(video_category_id, max_results=50):
    youtube = get_youtube()

    request = youtube.videos().list(
        part="snippet, statistics",
        chart="mostPopular",
        regionCode="US",
        videoCategoryId=video_category_id,
        maxResults=max_results,
    )

    videos = []

    try:
        response = request.execute()
    except Exception as e:
        print(f"Couldn't get popular videos due to exception {e}")
        return videos

    for video in response["items"]:
        videos.append(video)

    return videos


def get_video_comments(video_id, max_results=100):
    youtube = get_youtube()

    request = youtube.commentThreads().list(
        part="snippet", videoId=video_id, order="relevance", maxResults=max_results
    )

    comments = []

    try:
        response = request.execute()
    except Exception as e:
        print(f"Couldn't get comments due to exception {e}")
        return comments

    for item in response["items"]:
        comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        comments.append(comment)

    return comments


def get_video_data(video):
    video_data = {}

    video_data["video_id"] = video["id"]

    video_title = video["snippet"]["title"]
    video_data["video_title"] = video_title
    print(f"Video title: {video_title}")

    print("Getting entities for the title...")
    entities = get_ner(video_title)
    organization, person, location = [], [], []
    if isinstance(entities, list):
        for entity in entities:
            word = entity["word"]
            if (
                entity["entity_group"] == "ORG"
                and entity["score"] >= CLASSIFICATION_THRESHOLD
            ):
                if "#" not in word and len(word) >= 2:
                    organization.append(entity["word"])
            if (
                entity["entity_group"] == "PER"
                and entity["score"] >= CLASSIFICATION_THRESHOLD
            ):
                if "#" not in word and len(word) >= 2:
                    person.append(entity["word"])
            if (
                entity["entity_group"] == "LOC"
                and entity["score"] >= CLASSIFICATION_THRESHOLD
            ):
                if "#" not in word and len(word) >= 2:
                    location.append(entity["word"])

    if not (organization + person + location):
        print("Expected entities not found. Quitting...")
        return

    if organization:
        video_data["Organization"] = ", ".join(organization)
    else:
        video_data["Organization"] = NONE_FILLER
    if person:
        video_data["Person"] = ", ".join(person)
    else:
        video_data["Person"] = NONE_FILLER
    if location:
        video_data["Location"] = ", ".join(location)
    else:
        video_data["Location"] = NONE_FILLER

    print("Getting sub-category for the title...")
    sub_category = get_categories(video_title)
    if isinstance(sub_category, dict):
        title_sub_category_label = sub_category["labels"][0]
        title_sub_category_score = sub_category["scores"][0]

        if title_sub_category_score >= 0.25:
            video_data["sub_category"] = title_sub_category_label
        else:
            video_data["sub_category"] = "Miscellaneous"
    else:
        video_data["sub_category"] = "Miscellaneous"

    today = datetime.today()
    published_date = video["snippet"]["publishedAt"]
    published_date = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")
    published_date.strftime("%Y-%m-%d")
    total_days = today - published_date
    video_data["total_days"] = int(total_days.days)

    # Find alternate way to handle this
    video_statistics = video["statistics"]
    if "viewCount" in video_statistics:
        video_view_count = video_statistics["viewCount"]
    else:
        video_view_count = 0.0
    video_data["video_view_count"] = video_view_count

    if "likeCount" in video_statistics:
        video_like_count = video_statistics["likeCount"]
    else:
        video_like_count = 0.0
    video_data["video_like_count"] = video_like_count

    if "commentCount" in video_statistics:
        video_comment_count = video_statistics["commentCount"]
    else:
        video_comment_count = 0.0
    video_data["video_comment_count"] = video_comment_count

    return video_data


def process_video_data(video_id, video_title, comment_limit):
    video_data = {}

    print("Getting emotion for the title...")
    title_emotion = get_emotion(video_title)
    if isinstance(title_emotion, list):
        if title_emotion:
            title_emotion = title_emotion[0]
            title_emotion_prediction = title_emotion["label"]
            title_emotion_score = title_emotion["score"]
            if title_emotion_score >= CLASSIFICATION_THRESHOLD:
                video_data["title_emotion"] = title_emotion_prediction
            else:
                video_data["title_emotion"] = "neutral"

    print("Going over comments...")
    comment_count = 0
    top_level_comments = get_video_comments(video_id=video_id)
    comments_emotion_counter, comments = {}, []
    for top_level_comment in top_level_comments:
        comment_emotion = get_emotion(top_level_comment)

        if isinstance(comment_emotion, list):
            if comment_emotion:
                comment_emotion = comment_emotion[0]
                comment_emotion_prediction = comment_emotion["label"]

                # Neutral is abundant and not interesting
                if comment_emotion_prediction == "neutral":
                    continue

                comment_emotion_score = comment_emotion["score"]
                comment_emotion_score = round(comment_emotion_score, 2)
                if comment_emotion_score < CLASSIFICATION_THRESHOLD:
                    continue

                comments_emotion_counter[comment_emotion_prediction] = (
                    comments_emotion_counter.get(comment_emotion_prediction, 0) + 1
                )

        comments.append(top_level_comment)
        comment_count += 1
        if comment_count == comment_limit:
            break
    print(f"Found {len(comments)} comments")

    if comments_emotion_counter:
        video_data["comments_emotion"] = max(
            comments_emotion_counter, key=comments_emotion_counter.get
        )
    else:
        video_data["comments_emotion"] = "neutral"

    if comments:
        summary = get_sumy_summary(comments)
        video_data["comments_summary"] = summary
    else:
        video_data["comments_summary"] = NONE_FILLER

    return video_data
