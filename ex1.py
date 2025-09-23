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
    print(table_info)

    # Another way to show available columns and their types
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print("\ncolumns' names:", df.columns.tolist())
    print("columns' types:")
    print(df.dtypes)

    # Show count rows
    print("number of rows:", len(df))

    # Show first 5 rows
    print("\n", df.head())

# 1.2 =================================================================
lurkers_query = """
SELECT COUNT(*) AS lurkers_number FROM users
WHERE users.id NOT IN (SELECT DISTINCT user_id FROM reactions)
AND users.id NOT IN (SELECT DISTINCT user_id FROM comments)
AND users.id NOT IN (SELECT DISTINCT user_id FROM posts);
"""
lurkers_df = pd.read_sql_query(lurkers_query, conn)
print('===============================================================')
print('\nTask 1.2: Lurkers')
print(lurkers_df)

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

influencers_query = """
SELECT posts.user_id, users.username, COUNT(DISTINCT comments.id) + COUNT(DISTINCT reactions.id) AS engagement
FROM posts
LEFT JOIN comments ON posts.id = comments.post_id
LEFT JOIN reactions ON posts.id = reactions.post_id
JOIN users ON  posts.user_id = users.id
GROUP BY posts.user_id
ORDER BY engagement DESC
LIMIT 5;
"""

influencers_df = pd.read_sql_query(influencers_query, conn)
print('===============================================================')
print('\nTask 1.3: Influencers')
print(influencers_df)

# 1.4 =================================================================
# https://www.w3schools.com/sql/sql_union.asp
spammers_query = """
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

spammers_df = pd.read_sql_query(spammers_query, conn)
print('===============================================================')
print('\nTask 1.4: Spammers')
print(spammers_df)

# UNION ALL keeps duplicates, while UNION removes duplicates. But in this case ('true' AS is_post), there should not be duplicates anyway.

# Don't forget to close the database!!
conn.close()