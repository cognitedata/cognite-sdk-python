import re

# Functions
LIST_LIMIT_DEFAULT = 25
LIST_LIMIT_CEILING = 10_000  # variable used to guarantee all items are returned when list(limit) is None, inf or -1.
HANDLER_FILE_NAME = "handler.py"
MAX_RETRIES = 5
REQUIREMENTS_FILE_NAME = "requirements.txt"
REQUIREMENTS_REG = re.compile(r"(\[\/?requirements\]){1}$", flags=re.M)  # Matches [requirements] and [/requirements]
UNCOMMENTED_LINE_REG = re.compile(r"^[^\#]]*.*")
