import sys

import equalexperts_dataeng_exercise.db as db


def run_main_ingestion():
    try:
        db.remove_database()
        db.create_database()
        file_name = sys.argv[1]
        # file_name = "equalexperts_dataeng_exercise/my_uncommitted/votes_duplicate.jsonl"
        # file_name = "equalexperts_dataeng_exercise/my_uncommitted/votes_sample.jsonl"
        print(file_name)
        db.insert_data_into_database(file_name)
        db.display_data(3)
    except FileNotFoundError:
        print("Please download the dataset using 'poetry run exercise fetch-data'")


if __name__ == "__main__":
    print("Start main Ingestion")
    run_main_ingestion()
    print("End main Ingestion")
