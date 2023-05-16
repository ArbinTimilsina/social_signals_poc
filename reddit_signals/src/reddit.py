import os
from collections import defaultdict
import json

import praw
from huggingface import (
    EMOTION_MODEL_ID,
    NER_MODEL_ID,
    SENTIMENT_MODEL_ID,
    ESG_CATEGORIES_MODEL_ID,
    get_huggingface_response,
)

config = eval(os.environ["config"])
REDDIT_ID = config["reddit_id"]
REDDIT_SECRET = config["reddit_secret"]
REDDIT_USERNAME = config["reddit_username"]
REDDIT_PASSWORD = config["reddit_password"]

NER_ENTITY_THRESHOLD = 0.75
EMOTION_THRESHOLD = 0.75

COMMENTS_SPLITTER = " ||> "

def get_reddit():
    reddit = praw.Reddit(
        user_agent="SocialSignals/1.0",
        client_id=REDDIT_ID,
        client_secret=REDDIT_SECRET,
        username=REDDIT_USERNAME,
        password=REDDIT_PASSWORD,
    )
    return reddit


def get_subreddit(subreddit):
    reddit = get_reddit()
    subreddit = reddit.subreddit(subreddit)

    return subreddit


def get_top_submissions(subreddit, time_filter="day", limit=10):
    """
    time_filter: Can be one of: "all", "day", "hour", "month", "week", or "year"
    """
    subreddit = get_subreddit(subreddit)
    top_submissions = subreddit.top(time_filter=time_filter, limit=limit)

    return top_submissions


def get_comments(submission, comment_sort="top", comment_limit=10):
    """
    comment_sort: Can be one of: "confidence", "controversial", "new", "old", "q&a", and "top"
    """
    # Calling replace_more() access comments, and so must be done after comment_sort is updated
    submission.comment_sort = comment_sort
    submission.comment_limit = comment_limit

    # Remove comments like "load more comments”, and “continue this thread”
    submission.comments.replace_more(limit=0)

    comments = submission.comments
    return comments


def get_submission_data(submission, comment_sort="top", comment_limit=10):
    submission_data = {}

    submission_id = submission.id
    submission_data["submission_id"] = submission_id

    submission_url = submission.url
    submission_data["submission_url"] = submission_url

    submission_subreddit = submission.subreddit
    submission_data["submission_subreddit"] = submission_subreddit

    subreddit_subscribers = submission.subscribers
    submission_data["subreddit_subscribers"] = subreddit_subscribers

    submission_title = submission.title
    submission_data["submission_title"] = submission_title
    print(f"Submission title: {submission_title}")

    submission_score = submission.score
    submission_data["submission_score"] = submission_score
    print(f"Submission score: {submission_score}")

    print("Getting entities for the title...")
    huggingface_entities = get_huggingface_response(submission_title, NER_MODEL_ID)
    if isinstance(huggingface_entities, list):
        entities = defaultdict(list)
        for entity in huggingface_entities:
            if entity["entity_group"] == "ORG" and entity["score"] >= NER_ENTITY_THRESHOLD:
                entities["organization"].append(entity["word"])
            if entity["entity_group"] == "PER" and entity["score"] >= NER_ENTITY_THRESHOLD:
                entities["person"].append(entity["word"])
            if entity["entity_group"] == "LOC" and entity["score"] >= NER_ENTITY_THRESHOLD:
                entities["location"].append(entity["word"])

    entities = dict(entities)
    if len(entities) == 0:
        print("Expected entities not found. Quitting...")
        return
    submission_data["entities"] = json.dumps(entities, sort_keys=True)

    print("Getting sentiment for the title...")
    title_sentiment = get_huggingface_response(submission_title, SENTIMENT_MODEL_ID)
    if isinstance(title_sentiment, list):
        title_sentiment_prediction = title_sentiment[0][0]["label"]
        title_sentiment_score = title_sentiment[0][0]["score"]
        title_sentiment_score = round(title_sentiment_score, 2)
        submission_data["title_sentiment_prediction"] = title_sentiment_prediction
        submission_data["title_sentiment_score"] = title_sentiment_score

    print("Getting emotion for the title...")
    title_emotion = get_huggingface_response(submission_title, EMOTION_MODEL_ID)
    if isinstance(title_emotion, list):
        title_emotion_prediction = title_emotion[0][0]["label"]
        title_emotion_score = title_emotion[0][0]["score"]
        title_emotion_score = round(title_emotion_score, 2)
        submission_data["title_emotion_prediction"] = title_emotion_prediction
        submission_data["title_emotion_score"] = title_emotion_score

    print("Getting ESG categories for the title...")
    title_esg_categories = get_huggingface_response(
        submission_title, ESG_CATEGORIES_MODEL_ID
    )
    if isinstance(title_esg_categories, list):
        title_esg_categories_prediction = title_esg_categories[0][0]["label"]
        title_esg_categories_score = title_esg_categories[0][0]["score"]
        title_esg_categories_score = round(title_esg_categories_score, 2)
        submission_data[
            "title_esg_categories_prediction"
        ] = title_esg_categories_prediction
        submission_data["title_esg_categories_score"] = title_esg_categories_score

    submission_num_comments = submission.num_comments
    submission_data["submission_num_comments"] = submission_num_comments
    print(f"No. of comments: {submission_num_comments}")

    print("Going over comments...")
    top_level_comments = get_comments(
        submission=submission, comment_sort=comment_sort, comment_limit=comment_limit
    )
    comments_emotion_counter, comments = {}, []
    for top_level_comment in top_level_comments:
        comment = top_level_comment.body
        comments.append(comment)

        comment_emotion = get_huggingface_response(comment, EMOTION_MODEL_ID)

        if isinstance(comment_emotion, list):
            comment_emotion_prediction = comment_emotion[0][0]["label"]
            comment_emotion_score = comment_emotion[0][0]["score"]
            comment_emotion_score = round(comment_emotion_score, 2)
            if comment_emotion_score >= EMOTION_THRESHOLD:
                comments_emotion_counter[comment_emotion_prediction] = (
                    comments_emotion_counter.get(comment_emotion_prediction, 0) + 1
                )

    submission_data["comments"] = COMMENTS_SPLITTER.join(comments)
    submission_data["comments_emotion_counter"] = json.dumps(comments_emotion_counter, sort_keys=True)

    return submission_data
