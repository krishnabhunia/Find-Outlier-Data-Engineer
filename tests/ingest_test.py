import logging
import os
import subprocess
import time

import duckdb
import pytest
import equalexperts_dataeng_exercise.config as cfg

logger = logging.getLogger()


DB_NAME = cfg.DB_NAME
DB_SCHEMA_NAME = cfg.DB_SCHEMA_NAME
DB_TABLE_FULL_NAME = cfg.DB_TABLE_FULL_NAME


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)


def run_ingestion() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Running ingestion")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "tests/test-resources/samples-votes.jsonl",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def test_check_table_exists_and_names():
    run_ingestion()
    sql = """
        SELECT table_name, table_schema, table_catalog
        FROM information_schema.tables
        WHERE table_type LIKE '%TABLE' AND table_name='votes' AND table_schema='blog_analysis' AND table_catalog='warehouse';
    """
    con = duckdb.connect(DB_NAME, read_only=True)
    result = con.sql(sql)
    res_value = result.fetchall()[0]
    assert len(result.fetchall()) == 1, "Expected table 'votes' to exist"
    assert res_value[0] == "votes", "Expected table name to be 'votes'"
    assert res_value[1] == "blog_analysis", "Expected schema name to be 'blog_analysis'"
    assert res_value[2] == "warehouse", "Expected catalog name to be 'warehouse'"


def count_rows_in_data_file():
    with open("tests/test-resources/samples-votes.jsonl", "r", encoding="utf-8") as data:
        return sum(1 for _ in data)


def test_check_correct_number_of_rows_after_ingesting_once():
    sql = f"SELECT COUNT(*) FROM {DB_TABLE_FULL_NAME}"
    time_taken_seconds = run_ingestion()
    assert time_taken_seconds < 10, "Ingestion solution is too slow!"
    con = duckdb.connect(DB_NAME, read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert (
        count_in_db == count_rows_in_data_file()
    ), "Expect same count in db as in input file"


def test_check_correct_number_of_rows_after_ingesting_twice():
    sql = f"SELECT COUNT(*) FROM {DB_TABLE_FULL_NAME}"
    for _ in range(2):
        run_ingestion()
    con = duckdb.connect(DB_NAME, read_only=True)
    result = con.execute(sql)
    count_in_db = result.fetchall()[0][0]
    assert (
        count_in_db == count_rows_in_data_file()
    ), "Expect same count in db as in input file if processed twice"
