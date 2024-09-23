import equalexperts_dataeng_exercise.db as db

db.run_main_db()

# The following code is purely illustrative
try:
    pass
    # data = duckdb.read_json(sys.argv[1])
    # q = f"SELECT * FROM {sys.argv[1]}"
    # new_data = duckdb.sql(q)
    # with open(sys.argv[1]) as votes_in:
    #     for line in votes_in:
    #         print(json.loads(line))
    #         break
except FileNotFoundError:
    print("Please download the dataset using 'poetry run exercise fetch-data'")
