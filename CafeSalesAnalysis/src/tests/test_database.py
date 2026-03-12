import pandas as pd
import pytest
from unittest.mock import Mock
from src.sales_analysis import database

# ================= Helpers ========================

class DummyConnection:
    """Simple context-manager connection mock."""
    def __init__(self):
        self.execute = Mock()
        self.commit = Mock()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

class DummyEngine:
    """Simple engine mock with connect/dispose."""
    def __init__(self, connection=None):
        self._connection = connection or DummyConnection()
        self.dispose = Mock()
    def connect(self):
        return self._connection

# ============== Tests for connect_db ==============

def test_connect_db_success(monkeypatch):
    """
    Test that connect_db() successfully creates and returns a SQLAlchemy engine.
    This test mocks:
    - load_dotenv() so it does not read a real file
    - os.getenv() to return a fake connection string
    - sqlalchemy.create_engine() to return a mock engine
    """
    fake_engine = object()
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="postgresql://test"))
    monkeypatch.setattr(database.sa, "create_engine", Mock(return_value=fake_engine))
    result = database.connect_db()
    database.load_dotenv.assert_called_once()
    database.os.getenv.assert_called_once_with("CS")
    database.sa.create_engine.assert_called_once_with("postgresql://test")
    assert result is fake_engine

def test_connect_db_failure(monkeypatch):
    """
    Test that connect_db() handles an exception during engine creation.
    This simulates create_engine() failing and verifies:
    - An error is logged
    - The function returns None
    """
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="bad_connection_string"))
    monkeypatch.setattr(database.sa, "create_engine", Mock(side_effect=Exception("boom")))
    log_error = Mock()
    monkeypatch.setattr(database.logger, "error", log_error)
    result = database.connect_db()
    assert result is None
    log_error.assert_called_once()

# ================= Tests for drop_table =================

def test_drop_table_success(monkeypatch):
    """
    Test that drop_table() executes a DROP TABLE query successfully.
    Verifies that:
    - connect_db() is called
    - the query is executed
    - commit() is called
    - the engine is disposed
    """
    engine = DummyEngine()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(side_effect=lambda q: Mock(bindparams=Mock(return_value="DROP_QUERY"))))
    database.drop_table("sales")
    database.connect_db.assert_called_once()
    database.sa.text.assert_called_once_with("DROP TABLE IF EXISTS :tbl")
    engine._connection.execute.assert_called_once_with("DROP_QUERY")
    engine._connection.commit.assert_called_once()
    engine.dispose.assert_called_once()


def test_drop_table_failure(monkeypatch):
    """
    Test that drop_table() logs an error if executing the SQL query fails.
    """
    bad_conn = DummyConnection()
    bad_conn.execute.side_effect = Exception("sql failed")
    engine = DummyEngine(connection=bad_conn)
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(side_effect=lambda q: Mock(bindparams=Mock(return_value="DROP_QUERY"))))
    log_error = Mock()
    monkeypatch.setattr(database.logger, "error", log_error)
    database.drop_table("sales")
    log_error.assert_called_once()
    engine.dispose.assert_called_once()

# =========== Tests for write_from_dataframe ================

def test_write_from_dataframe_success(monkeypatch):
    """
    Test that write_from_dataframe() writes a dataframe to a table.
    Verifies that:
    - connect_db() is called
    - dataframe.to_sql() is executed
    - the engine is disposed after writing
    """
    df = pd.DataFrame({"item": ["coffee"], "price": [4.50]})
    engine = DummyEngine()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    to_sql_mock = Mock()
    monkeypatch.setattr(pd.DataFrame, "to_sql", to_sql_mock, raising=True)
    database.write_from_dataframe(df, "sales")
    database.connect_db.assert_called_once()
    to_sql_mock.assert_called_once()
    args = to_sql_mock.call_args[0]
    assert args[0] == "sales"
    engine.dispose.assert_called_once()

def test_write_from_dataframe_empty_df_logs_warning(monkeypatch):
    """
    Test that a warning is logged if an empty dataframe is written.
    The function should still attempt to create the table.
    """
    df = pd.DataFrame()
    engine = DummyEngine()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    warn_mock = Mock()
    monkeypatch.setattr(database.logger, "warning", warn_mock)
    to_sql_mock = Mock()
    monkeypatch.setattr(pd.DataFrame, "to_sql", to_sql_mock, raising=True)
    database.write_from_dataframe(df, "sales")
    warn_mock.assert_called_once()
    to_sql_mock.assert_called_once()
    engine.dispose.assert_called_once()

def test_write_from_dataframe_failure(monkeypatch):
    """
    Test that write_from_dataframe() logs an error if writing fails.
    """
    df = pd.DataFrame({"item": ["cake"]})
    engine = DummyEngine()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(pd.DataFrame, "to_sql", Mock(side_effect=Exception("write failed")), raising=True)
    log_error = Mock()
    monkeypatch.setattr(database.logger, "error", log_error)
    database.write_from_dataframe(df, "sales")
    log_error.assert_called_once()
    engine.dispose.assert_called_once()

# ============= Tests for read_as_dataframe ================

def test_read_as_dataframe_success(monkeypatch):
    """
    Test that read_as_dataframe() successfully reads data from a table
    and returns it as a pandas dataframe.
    """
    engine = DummyEngine()
    expected_df = pd.DataFrame({"item": ["coffee"], "price": [4.5]})
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(side_effect=lambda q: Mock(bindparams=Mock(return_value="SELECT_QUERY"))))
    monkeypatch.setattr(database.pd, "read_sql", Mock(return_value=expected_df))
    result = database.read_as_dataframe("sales")
    database.connect_db.assert_called_once()
    database.sa.text.assert_called_once_with("SELECT * FROM :tbl")
    database.pd.read_sql.assert_called_once_with("SELECT_QUERY", engine._connection)
    engine.dispose.assert_called_once()
    pd.testing.assert_frame_equal(result, expected_df)

def test_read_as_dataframe_failure(monkeypatch):
    """
    Test that read_as_dataframe() logs an error if the SQL query fails.
    """
    engine = DummyEngine()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(side_effect=lambda q: Mock(bindparams=Mock(return_value="SELECT_QUERY"))))
    monkeypatch.setattr(database.pd, "read_sql", Mock(side_effect=Exception("read failed")))
    log_error = Mock()
    monkeypatch.setattr(database.logger, "error", log_error)
    result = database.read_as_dataframe("sales")
    assert result is None
    log_error.assert_called_once()
    engine.dispose.assert_called_once()