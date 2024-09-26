import os
import duckdb
import equalexperts_dataeng_exercise.config as cfg

DB_NAME = cfg.DB_NAME
DB_SCHEMA_NAME = cfg.DB_SCHEMA_NAME


def create_database_and_schema():
    try:
        # Connect to the database
        conn = duckdb.connect(DB_NAME)
        print(f"Created and connected to the database: '{DB_NAME}'")

        # Create schema 'blog_analysis' if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA_NAME}")
        print(f"Created schema :'{DB_SCHEMA_NAME}' under Database : '{DB_NAME}'")
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        conn.close()


def remove_database():
    try:
        if os.path.exists(DB_NAME):
            os.remove(DB_NAME)
            print(f"Database '{DB_NAME}' deleted successfully.")
        else:
            print(f"Database '{DB_NAME}' does not exist.")
    except Exception as ex:
        print(f"Error : {ex}")


def run_main_db():
    remove_database()
    create_database_and_schema()


if __name__ == "__main__":
    print("Start main db")
    run_main_db()
    print("End main db")
