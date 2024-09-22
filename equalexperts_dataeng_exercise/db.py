import duckdb


def create_database():
    try:
        # Connect to the database
        conn = duckdb.connect("warehouse.db")
        print("Connected to the database: successfully")
        
        # Create a new table in the database
        conn.execute('''
            CREATE TABLE votes (
                Id INTEGER,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            );
        ''')
        print("Table created successfully")

        # Close the connection
        conn.close()
    # except duckdb.database.DatabaseError as ex:
    #     print(f"Error creating database: {ex}")
    except Exception as ex:
        print(f"Error : {ex}")


def insert_data_into_database():
    pass


def display_data(rows=3):
    conn = duckdb.connect("warehouse.db")
    cursor = conn.cursor()
    sql_query = f"SELECT * FROM votes LIMIT {rows}"
    cursor.execute(sql_query)
    
    # Fetch the first 3 rows
    rows = cursor.fetchmany(3)
    
    # Print the fetched rows
    for row in rows:
        print(row)


def run_main_db():
    create_database()
    insert_data_into_database()
    display_data()


if __name__ == "__main__":
    print("Start main db")
    run_main_db()
    print("End main db")
