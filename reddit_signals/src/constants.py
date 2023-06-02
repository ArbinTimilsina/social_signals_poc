# Weights for Social Signals rank
SUBMISSION_WEIGHT = 0.35
COMMENT_WEIGHT = 0.65

# For submission data
SUBMISSION_TIME_FILTER = "day"
SUBMISSION_LIMIT = 15

# For comments data
COMMENT_SORT = "top"
COMMENT_LIMIT = 15

# For DB
SCHEMA = "social_signals_dev"
TABLE_NAME = "social_signals_poc"

CLASSIFICATION_THRESHOLD = 0.60
NONE_FILLER = "0"

# Entities
ENTITIES = ["organization", "person", "location"]

# For categories
CATEGORIES = [
    "News",
    "Politics",
    "Science",
    "Technology",
    "Sports",
    "Movies",
    "Television",
    "Entertainment",
    "Education",
    "Health",
    "Music",
    "Finance",
    "Miscellaneous",
]