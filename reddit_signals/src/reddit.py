import os

import praw
from praw.models import MoreComments

from common_tools.common_constants import CLASSIFICATION_THRESHOLD, NONE_FILLER
from common_tools.open_ai import get_openai_summary
from common_tools.sagemaker_inference import get_categories, get_emotion, get_ner

config = eval(os.environ["config"])
REDDIT_ID = config["reddit_id"]
REDDIT_SECRET = config["reddit_secret"]
REDDIT_USERNAME = config["reddit_username"]
REDDIT_PASSWORD = config["reddit_password"]


def get_reddit():
    reddit = praw.Reddit(
        user_agent="SocialSignals/1.0",
        client_id=REDDIT_ID,
        client_secret=REDDIT_SECRET,
        username=REDDIT_USERNAME,
        password=REDDIT_PASSWORD,
    )
    return reddit


def get_subreddit(subreddit_name):
    reddit = get_reddit()
    subreddit = reddit.subreddit(subreddit_name)

    return subreddit


def get_top_submissions(subreddit_name, time_filter="day"):
    """
    time_filter: Can be one of: "all", "day", "hour", "month", "week", or "year"
    """
    subreddit = get_subreddit(subreddit_name)
    top_submissions = subreddit.top(time_filter=time_filter)

    return subreddit, top_submissions


def get_comments(submission, comment_sort="top"):
    """
    comment_sort: Can be one of: "confidence", "controversial", "new", "old", "q&a", and "top"
    """
    # Calling replace_more() access comments, and so must be done after comment_sort is updated
    submission.comment_sort = comment_sort

    # Remove comments like "load more comments”, and “continue this thread”
    submission.comments.replace_more(limit=0)

    comments = submission.comments
    return comments


def get_submission_data(subreddit, submission):
    submission_data = {}

    subreddit_name = subreddit.display_name
    submission_data["subreddit_name"] = subreddit_name

    submission_id = submission.id
    submission_data["submission_id"] = submission_id

    submission_title = submission.title
    submission_data["submission_title"] = submission_title
    print(f"Submission title: {submission_title}")

    print("Getting entities for the title...")
    entities = get_ner(submission_title)
    organization, person, location = [], [], []
    if isinstance(entities, list):
        for entity in entities:
            # TO DO: Find better fix if word is corrupted due to tokenization
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
        submission_data["Organization"] = ", ".join(organization)
    else:
        submission_data["Organization"] = NONE_FILLER
    if person:
        submission_data["Person"] = ", ".join(person)
    else:
        submission_data["Person"] = NONE_FILLER
    if location:
        submission_data["Location"] = ", ".join(location)
    else:
        submission_data["Location"] = NONE_FILLER

    print("Getting sub-category for the title...")
    sub_category = get_categories(submission_title)
    if isinstance(sub_category, dict):
        title_sub_category_label = sub_category["labels"][0]
        title_sub_category_score = sub_category["scores"][0]

        if title_sub_category_score >= 0.25:
            submission_data["sub_category"] = title_sub_category_label
        else:
            submission_data["sub_category"] = "Miscellaneous"
    else:
        submission_data["sub_category"] = "Miscellaneous"

    subreddit_subscribers = subreddit.subscribers
    submission_data["subreddit_subscribers"] = subreddit_subscribers

    submission_score = submission.score
    submission_data["submission_score"] = submission_score

    submission_num_comments = submission.num_comments
    submission_data["submission_num_comments"] = submission_num_comments

    return submission_data


def process_submission_data(
    submission_id, submission_title, comment_sort="top", comment_limit=10
):
    reddit = get_reddit()
    submission = reddit.submission(submission_id)

    assert submission.title == submission_title, "Miss-match in submission title!"

    submission_data = {}

    print("Getting emotion for the title...")
    title_emotion = get_emotion(submission_title)
    if isinstance(title_emotion, list):
        if title_emotion:
            title_emotion = title_emotion[0]
            title_emotion_prediction = title_emotion["label"]
            title_emotion_score = title_emotion["score"]
            if title_emotion_score >= CLASSIFICATION_THRESHOLD:
                submission_data["title_emotion"] = title_emotion_prediction
            else:
                submission_data["title_emotion"] = "neutral"

    print("Going over comments...")
    comment_count = 0
    top_level_comments = get_comments(submission=submission, comment_sort=comment_sort)
    comments_emotion_counter, comments = {}, []
    for top_level_comment in top_level_comments:
        if isinstance(top_level_comment, MoreComments):
            continue

        # We don't want stickied comments- mostly from Mods
        if top_level_comment.stickied:
            print("Found stickied comment; skipping...")
            continue

        # We don't want comments from bots
        comment_author = top_level_comment.author
        if comment_author:
            if "bot" in comment_author.name.lower():
                print(f"Found comment from bot {comment_author}; skipping...")
                continue

        comment = top_level_comment.body
        comment_emotion = get_emotion(comment)

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

        comments.append(comment)
        comment_count += 1
        if comment_count == comment_limit:
            break
    print(f"Found {len(comments)} comments")

    if comments_emotion_counter:
        submission_data["comments_emotion"] = max(
            comments_emotion_counter, key=comments_emotion_counter.get
        )
    else:
        submission_data["comments_emotion"] = "neutral"

    if comments:
        summary_text = " ".join(comments)
        summary = get_openai_summary(summary_text)

        submission_data["comments_summary"] = summary
    else:
        submission_data["comments_summary"] = NONE_FILLER

    return submission_data
