# SQLite library for python for reading, updating database
# See documentation at https://docs.python.org/3/library/sqlite3.html
import sqlite3
import math

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
# --- From In-class Exercise, Task 9 ---
# Measure historical growth in platform activity (e.g., posts, posts and their interactions, users, or active users).
# To keep it simple, let's just look at posts.

# 2.1.1. Monthly activity by posts created
growth_of_posts_query = """
SELECT
    STRFTIME('%Y-%m', p.created_at) AS month,
    COUNT(p.id) AS monthly_posts,
    SUM(COUNT(p.id)) OVER (ORDER BY STRFTIME('%Y-%m', p.created_at)) AS cumulative_posts
FROM posts p
GROUP BY month
ORDER BY month;
"""
monthly_counts = pandas.read_sql_query(growth_of_posts_query, conn)
print("\n--- Trend: Monthly and Cumulative Posts for growth_of_posts_query ---")
print(monthly_counts)

# 2.1.2. Plot
plt.plot(monthly_counts['month'], monthly_counts['cumulative_posts'])
plt.title("Cumulative Posts Over Time")
plt.xlabel("Month")
plt.ylabel("Total Cumulative Posts")
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# 2.1.3. Naive growth estimate
# Average monthly growth rate in posts
current_total = monthly_counts['cumulative_posts'].iloc[-1]
n_months = len(monthly_counts)
avg_growth = current_total / n_months

# Current servers' capacity
current_servers = 16
capacity_per_server = current_total / current_servers

# Predict number of servers needed in 3 years (36 months) if growth continues at the same rate
required_capacity = avg_growth * 36
required_capacity_redundancy = required_capacity * 1.2  # plus 20% capacity for redundancy
total_needed_servers = (required_capacity_redundancy + current_total) / capacity_per_server

print(f"\nPrediction: Based on avg {avg_growth:.2f} posts/month historical growth, "
      f"It needs about {math.ceil(total_needed_servers)} servers to next 3 years (+20% redundancy).")

# 2.2.4. Refined approach by removing outliers (using 1.5 * IQR rule)
# December and June of each year have a high number of posts, so it's not too unusual, however we can still remove outliers for both lower_bound and upper_bound
q1 = monthly_counts['monthly_posts'].quantile(0.25)
q3 = monthly_counts['monthly_posts'].quantile(0.75)
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

print(f"\nOutlier removal bounds:\n\tLower: {lower_bound:.2f}\n\tUpper: {upper_bound:.2f}")

# Filter to exclude outliers
filtered = monthly_counts[
    (monthly_counts['monthly_posts'] >= lower_bound) &
    (monthly_counts['monthly_posts'] <= upper_bound)
]

# Recalculate growth estimate without outliers
filtered_total = filtered['monthly_posts'].sum()
n_filtered_months = len(filtered)
avg_growth_filtered = filtered_total / n_filtered_months if n_filtered_months > 0 else 0

# Predict number of servers needed in 3 years (36 months) if growth continues at the same rate
required_capacity_filtered = avg_growth_filtered * 36
required_capacity_redundancy_filtered = required_capacity_filtered * 1.2  # plus 20% capacity for redundancy
total_needed_servers_filtered = (required_capacity_redundancy_filtered + current_total) / capacity_per_server

print(f"\nRefined Prediction (outliers removed): Based on avg {avg_growth_filtered:.2f} posts/month historical growth, "
      f"It needs about {math.ceil(total_needed_servers_filtered)} servers to next 3 years (+20% redundancy).")

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
# Note that engagement having created_at is comments not reactions. Both comments and reactions's created_at is not null as setting default.
# Find min created_at of comments for each post, then calculate the average of gap between post time and first comment time.
# By minute
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

# 2.4 =================================================================

# Don't forget to close the database!!
conn.close()