import json
import sys
import duckdb

VIEW_NAME = "outlier_weeks"
DB_SCHEMA_NAME = "blog_analysis"

# The following code is purely illustrative
try:
    print(sys.argv[1])
    data = duckdb.read_json(sys.argv[1])
    print(data)
    print(type(data))
    q = f"SELECT * FROM {sys.argv[1]}"
    print(q)
    new_data = duckdb.sql(q)
    print(new_data)
    with open(sys.argv[1]) as votes_in:
        for line in votes_in:
            print(json.loads(line))
            print('like break')
            break
except FileNotFoundError:
    print("Please download the dataset using 'poetry run exercise fetch-data'")
