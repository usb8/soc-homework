# SQLite library for python for reading, updating database
# See documentation at https://docs.python.org/3/library/sqlite3.html
import sqlite3

# Pandas library for data analysis and manipulation. Not specific for SQL, but provides support for importing data from SQLite database file.
# See documentation at https://pypi.org/project/pandas/
import pandas

# A very powerful visualisation library for python: https://pypi.org/project/matplotlib/
import matplotlib.pyplot as plt

DB_FILE = "minisocial_database.sqlite"
try:
    conn = sqlite3.connect(DB_FILE)
    print("SQLite Database connection successful")
except Exception as e:
    print(f"Error: '{e}'")

# 2.1 =================================================================

# 2.2 =================================================================
"""
SELECT posts.id, posts.content, COUNT(DISTINCT comments.id) + COUNT(DISTINCT reactions.id) AS interactions
FROM posts
LEFT JOIN comments ON posts.id = comments.post_id
LEFT JOIN reactions ON posts.id = reactions.post_id
GROUP BY posts.id
ORDER BY interactions DESC
LIMIT 3;
"""
# Like ex 1.3

# 2.3 =================================================================
"""
-- SELECT AVG(first_comment_time - post_time)
SELECT AVG((strftime('%s', first_comment_time) - strftime('%s', post_time))/60)
FROM (
    SELECT posts.id, posts.created_at AS post_time, MIN(comments.created_at) AS first_comment_time
    FROM posts
    LEFT JOIN comments ON posts.id = comments.post_id
    GROUP BY posts.id
);
"""

"""
-- SELECT AVG(last_comment_time - post_time)
SELECT AVG((strftime('%s', last_comment_time) - strftime('%s', post_time))/60)
FROM (
    SELECT posts.id, posts.created_at AS post_time, MAX(comments.created_at) AS last_comment_time
    FROM posts
    LEFT JOIN comments ON posts.id = comments.post_id
    GROUP BY posts.id
);
"""
# Note that engagement having created_at is comments not reactions. Both comments and reactions's created_at is not null as setting default.
# Find min created_at of comments for each post, then calculate the average of gap between post time and first comment time.
# By minute

# 2.4 =================================================================