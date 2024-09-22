import duckdb
from datetime import datetime
from collections import defaultdict
import db

db.run_main_db()

DB_NAME = "warehouse.db"
DB_SCHEMA_NAME = "blog_analysis"
DB_TABLE_NAME = "votes"
DB_TABLE_FULL_NAME = f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME}"
FILE_NAME = "uncommitted/votes_test.jsonl"

try:
    conn = duckdb.connect("warehouse.db")
    cursor = conn.cursor()
    sql_query = f"SELECT * FROM {DB_TABLE_FULL_NAME}"
    data_rows = cursor.execute(sql_query).fetchall()

    # Print the fetched rows
    # for row in data_rows:
    #     date_str = str(row[3])
    #     print(row, date_str)
    #     date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    #     week_number = date_obj.isocalendar()[1]
        # if week_number >= 52:
        #     week_number = week_number - 52
        # print(f"Week number: {week_number}, and data_obj {date_obj.isocalendar()}")

    # new code
    week_count = defaultdict(int)

    # Iterate over the list of date strings
    for row in data_rows:
        # Extract the year and week number
        date_obj = row[3]
        # print(f"date object {date_obj}, and iso {date_obj.isocalendar()}")
        year, week_number, _ = date_obj.isocalendar()

        if year != date_obj.year:
            print(f"Year mismatch: {year} != {date_obj.year}")
            year = date_obj.year
            print(f"identified week:{week_number}")
            if week_number >= 52:
                week_number = 0
            elif week_number == 0:
                week_number = 52

            print(f"Final data after conversion {year, week_number}")
        
        # Increment the count for the (year, week_number) combination
        week_count[(year, week_number)] += 1

    # Convert the dictionary to a list of tuples with (year, week_number, count)
    week_list = [(year, week_number, count) for (year, week_number), count in week_count.items()]

    year_summary = defaultdict(lambda: {'total_weeks': 0, 'sum_votes': 0})

    # Aggregate data per year
    for (year, week_number), count in week_count.items():
        year_summary[year]['total_weeks'] += 1  # Count unique weeks
        year_summary[year]['sum_votes'] += count  # Sum of occurrences

    # Print the results for each year
    for year, summary in year_summary.items():
        print(f"Year: {year}, Total Weeks: {summary['total_weeks']}, Sum of Counts: {summary['sum_votes']}")

    # Print the result
    # print(week_list)
    for w in week_list:
        w.append("hello")
        print(w)
    print(year_summary)
    # print(week_list)

except Exception as ex:
    print(f"Error : {ex}")
finally:
    conn.close()
