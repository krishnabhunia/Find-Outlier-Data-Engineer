import duckdb
import logging
import os
import subprocess
import time

# import duckdb
import pytest
import equalexperts_dataeng_exercise.config as cfg


DB_NAME = cfg.DB_NAME
DB_SCHEMA_NAME = cfg.DB_SCHEMA_NAME
DB_TABLE_FULL_NAME = cfg.DB_TABLE_FULL_NAME

logger = logging.getLogger()


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)


def run_db() -> float:
    """
    Returns time in seconds that the ingestion process took to run
    """
    logger.info("Deleting and creating DB")
    tic = time.perf_counter()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.db",
        ],
        capture_output=True,
    )
    toc = time.perf_counter()
    result.check_returncode()
    return toc - tic


def test_duckdb_connection():
    cursor = duckdb.connect("warehouse.db")
    assert list(cursor.execute("SELECT 1").fetchall()) == [(1,)]


def test_check_table_exists_and_names():
    run_db()
    sql = f"""
        SELECT catalog_name, schema_name
        FROM information_schema.schemata
        WHERE schema_name = '{DB_SCHEMA_NAME}' AND catalog_name='warehouse';
    """
    con = duckdb.connect(DB_NAME, read_only=True)
    result = con.sql(sql)
    res_value = result.fetchall()[0]
    assert len(result.fetchall()) == 1, "Expected table 'votes' to exist"
    assert res_value[0] == "warehouse", "Expected catalog name to be 'warehouse'"
    assert res_value[1] == "blog_analysis", "Expected schema name to be 'blog_analysis'"