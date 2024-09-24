import subprocess

import duckdb
import equalexperts_dataeng_exercise.config as cfg

VIEW_NAME = cfg.VIEW_NAME
DB_SCHEMA_NAME = cfg.DB_SCHEMA_NAME
DB_NAME = cfg.DB_NAME


def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "equalexperts_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()


def test_check_view_exists():
    sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type='VIEW' AND table_name='{VIEW_NAME}' AND table_schema='{DB_SCHEMA_NAME}' AND table_catalog='warehouse';
    """
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) == 1, "Expected view 'outlier_weeks' to exist"
    finally:
        con.close()


def test_check_view_has_data():
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    run_outliers_calculation()
    con = duckdb.connect(DB_NAME, read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) > 0, "Expected view 'outlier_weeks' to have data"
    finally:
        con.close()
