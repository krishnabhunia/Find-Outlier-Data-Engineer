import os

import duckdb

DB_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "votes"
DB_TABLE_FULL_NAME = f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME}"


def create_database():
    try:
        # Connect to the database
        conn = duckdb.connect(DB_NAME)
        print(f"Connected to the database: {DB_NAME}")

        # Create schema 'blog_analysis' if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA_NAME}")
        print(f"Created schema :{DB_SCHEMA_NAME}")

        # Create a new table in the database
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {DB_TABLE_FULL_NAME}(
                Id INTEGER PRIMARY KEY,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            );
        ''')
        print(f"Table created successfully:{DB_TABLE_FULL_NAME}")
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        # Close the connection
        conn.close()


def insert_data_into_database(file_name):
    try:
        print(f"Inserting data from file: {file_name}")
        insert = f"INSERT INTO {DB_TABLE_FULL_NAME} \
            SELECT Distinct(Id), PostId, VoteTypeId, CreationDate FROM '{file_name}'"

        # print("Insert query:", insert)

        conn = duckdb.connect("warehouse.db")
        res = conn.execute(insert).fetchall()

        print(
            f"Data inserted successfully:{DB_TABLE_FULL_NAME} and number of rows:{res[0][0]}")
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        # Close the connection
        conn.close()


def display_data(rows=3):
    try:
        conn = duckdb.connect("warehouse.db")
        cursor = conn.cursor()
        sql_query = f"SELECT * FROM {DB_TABLE_FULL_NAME} ORDER BY Id DESC LIMIT {rows}"
        data_rows = cursor.execute(sql_query).fetchall()

        # Print the fetched rows
        for row in data_rows:
            print(row)
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        # Close the connection
        conn.close()


def remove_database():
    db_path = DB_NAME
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Database '{db_path}' deleted successfully.")
    else:
        print(f"Database '{db_path}' does not exist.")


def run_main_db():
    remove_database()
    create_database()
    insert_data_into_database()
    display_data(3)


if __name__ == "__main__":
    print("Start main db")
    run_main_db()
    print("End main db")
