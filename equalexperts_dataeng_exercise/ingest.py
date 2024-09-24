import sys
import duckdb
import equalexperts_dataeng_exercise.db as db
import equalexperts_dataeng_exercise.config as cfg


DB_NAME = cfg.DB_NAME
DB_SCHEMA_NAME = cfg.DB_SCHEMA_NAME
DB_TABLE_FULL_NAME = cfg.DB_TABLE_FULL_NAME


def create_table():
    try:
        # Connect to the database and create table
        conn = duckdb.connect(DB_NAME)
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {DB_TABLE_FULL_NAME}(
                Id INTEGER PRIMARY KEY,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            );
        ''')
        print(f"Table created successfully : '{DB_TABLE_FULL_NAME}'")
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        conn.close()


def insert_data_into_database(file_name):
    try:
        print(f"Inserting data from file: {file_name}")
        insert = f"""INSERT INTO {DB_TABLE_FULL_NAME}
            SELECT Distinct(Id), PostId, VoteTypeId, CreationDate FROM '{file_name}'"""

        # print("Insert query:", insert)

        conn = duckdb.connect(DB_NAME)
        res = conn.execute(insert).fetchall()

        print(
            f"Data inserted successfully:'{DB_TABLE_FULL_NAME}' and number of rows:{res[0][0]}")
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        # Close the connection
        conn.close()


def display_data(rows=3):
    try:
        conn = duckdb.connect(DB_NAME)
        cursor = conn.cursor()
        sql_query = f"SELECT * FROM {DB_TABLE_FULL_NAME} ORDER BY Id DESC LIMIT {rows}"
        data_rows = cursor.execute(sql_query).fetchall()

        # Print the fetched rows
        print(f"Displaying {rows} rows from the table : {DB_TABLE_FULL_NAME}")
        for row in data_rows:
            print(row)
    except Exception as ex:
        print(f"Error : {ex}")
    finally:
        # Close the connection
        conn.close()


def run_main_ingestion():
    try:
        db.run_main_db()
        create_table()
        file_name = sys.argv[1]
        # file_name = "equalexperts_dataeng_exercise/my_uncommitted/votes_duplicate.jsonl"
        # file_name = "equalexperts_dataeng_exercise/my_uncommitted/votes_sample.jsonl"
        print(f"File name :{file_name}")
        insert_data_into_database(file_name)
        display_data(3)
    except FileNotFoundError:
        print("Please download the dataset using 'poetry run exercise fetch-data'")


if __name__ == "__main__":
    print("Start main Ingestion")
    run_main_ingestion()
    print("End main Ingestion")
