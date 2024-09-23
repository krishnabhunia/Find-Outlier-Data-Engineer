import duckdb

# import db

# db.run_main_db()

DB_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "votes"
VIEW_NAME = "outlier_weeks"
FULL_VIEW_NAME = F"{DB_SCHEMA_NAME}.{VIEW_NAME}"
DB_TABLE_FULL_NAME = f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME}"
FILE_NAME = "uncommitted/votes_test.jsonl"

# List of datetime objects (You can replace this with your actual list)
conn = duckdb.connect("warehouse.db")
cursor = conn.cursor()

sql_query = f"DROP VIEW IF EXISTS {FULL_VIEW_NAME}; \
CREATE VIEW {FULL_VIEW_NAME} AS \
WITH yearly_vote_count AS ( \
SELECT  \
    EXTRACT(YEAR FROM CreationDate) AS year,\
    COUNT(DISTINCT EXTRACT(WEEK FROM CreationDate)) AS total_weeks,\
    COUNT(Id) AS total_vote_count, \
FROM {DB_TABLE_FULL_NAME}\
    GROUP BY year \
    ), \
weekly_vote_count AS ( \
    SELECT \
        EXTRACT(YEAR FROM CreationDate) AS year, \
        CASE \
            WHEN EXTRACT(WEEK FROM CreationDate) = 52 THEN 0 \
            ELSE EXTRACT(WEEK FROM CreationDate) \
        END AS week_number,\
        COUNT(*) AS weekly_vote_count \
    FROM {DB_TABLE_FULL_NAME} \
    GROUP BY year, week_number \
), \
outlier_cal AS ( \
select w.year, w.week_number, w.weekly_vote_count,  \
    ABS(1 - (w.weekly_vote_count / (y.total_vote_count / y.total_weeks))) AS weekly_vote_ratio, \
    CASE \
        WHEN weekly_vote_ratio > 0.2 then TRUE \
        ELSE FALSE \
    END as filter_param \
        from weekly_vote_count w join yearly_vote_count y on w.year = y.year )\
SELECT * FROM outlier_cal \
WHERE filter_param = TRUE \
"
summary = cursor.execute(sql_query).fetchall()

view_query = f"select Year as Year, week_number as WeekNumber, weekly_vote_count as VoteCount from {FULL_VIEW_NAME}"

outlier_table = cursor.execute(view_query).fetchall()

print("summary")
for r in summary:
    print("summary:", r)

column_names = [desc[0] for desc in cursor.description]

# Print column names
print(column_names)

for r in outlier_table:
    print(r)
