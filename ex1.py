# SQLite library for python for reading, updating database
# See documentation at https://docs.python.org/3/library/sqlite3.html
import sqlite3

# Pandas library for data analysis and manipulation. Not specific for SQL, but provides support for importing data from SQLite database file.
# Seed documentation at https://pypi.org/project/pandas/
import pandas as pd

# # Optional for clearing the terminal at each run
# import os
# os.system('clear')

# print("Hi")

# 1.1 =================================================================
# Connect to SQLite db
DB_FILE = "minisocial_database.sqlite"
try:
    conn = sqlite3.connect(DB_FILE)
    print("SQLite Database connection successful")
except Exception as e:
    print(f"Error: '{e}'")

tablenames_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
print(tablenames_df)

for table in ['users', 'posts', 'comments', 'reactions', 'follows']:
    print(f'\n\ncontents of table {table} --------------------------------')

    # Show available columns and their types
    table_info = pd.read_sql_query(f"PRAGMA table_info({table})", conn)  # https://www.sqlitetutorial.net/sqlite-describe-table/
    # table_info = pd.read_sql_query(f"SELECT sql FROM sqlite_schema WHERE name = '{table}'", conn)  # https://www.sqlitetutorial.net/sqlite-describe-table/
    print(table_info)

    # Show count rows
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print("number of rows:", len(df))
    # Show first 5 rows
    print(df.head())

# 1.2 =================================================================
"""
SELECT COUNT(*) AS lurkers_number FROM users
WHERE users.id NOT IN (SELECT DISTINCT user_id FROM reactions)
AND users.id NOT IN (SELECT DISTINCT user_id FROM comments)
AND users.id NOT IN (SELECT DISTINCT user_id FROM posts);
"""

# 1.3 =================================================================
"""
-- SELECT posts.user_id,COUNT(posts.user_id) as engagement
-- SELECT posts.user_id, COUNT(comments.id) + COUNT(reactions.id) AS engagement
SELECT posts.user_id, COUNT(DISTINCT comments.id) + COUNT(DISTINCT reactions.id) AS engagement
FROM posts
LEFT JOIN comments ON posts.id = comments.post_id
LEFT JOIN reactions ON posts.id = reactions.post_id
GROUP BY posts.user_id
ORDER BY engagement DESC
LIMIT 5;
"""

"""
SELECT posts.user_id, users.username, COUNT(DISTINCT comments.id) + COUNT(DISTINCT reactions.id) AS engagement
FROM posts
LEFT JOIN comments ON posts.id = comments.post_id
LEFT JOIN reactions ON posts.id = reactions.post_id
JOIN users ON  posts.user_id = users.id
GROUP BY posts.user_id
ORDER BY engagement DESC
LIMIT 5;
"""

# 1.4 =================================================================
# https://www.w3schools.com/sql/sql_union.asp
"""
SELECT user_id, content AS post_or_comment_content, 'true' AS is_post, COUNT(*) AS frequency
FROM posts
GROUP BY user_id, content
HAVING COUNT(*) >= 3
UNION
SELECT user_id, content AS post_or_comment_content, 'false' AS is_post, COUNT(*) AS frequency
FROM comments
GROUP BY user_id, content
HAVING COUNT(*) >= 3;
"""

# Don't forget to close the database!!
conn.close()