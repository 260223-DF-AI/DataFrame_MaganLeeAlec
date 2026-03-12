## pytesting for database.py
import pytest
import pandas as pd
from unittest.mock import Mock
import src.sales_analysis.database as database


# ================= helper mock classes =================

class DummyConnection:
    """Fake database connection used for testing."""
    def __init__(self):
        self.execute = Mock()
        self.commit = Mock()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False


class DummyEngine:
    """Fake SQLAlchemy engine."""
    def __init__(self, connection=None):
        self._connection = connection or DummyConnection()
        self.dispose = Mock()
    def connect(self):
        return self._connection


# ================= tests for connect_db =================

# 1. test that connect_db returns an engine
def test_connect_db_returns_engine(monkeypatch):
    fake_engine = object()
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="postgresql://test"))
    monkeypatch.setattr(database.sa, "create_engine", Mock(return_value=fake_engine))
    engine = database.connect_db()
    assert engine is fake_engine, "connect_db should return the created database engine"


# 2. test that connect_db loads environment variables
def test_connect_db_calls_load_dotenv(monkeypatch):
    load_mock = Mock()
    monkeypatch.setattr(database, "load_dotenv", load_mock)
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="postgresql://test"))
    monkeypatch.setattr(database.sa, "create_engine", Mock(return_value=object()))
    database.connect_db()
    assert load_mock.called, "connect_db should call load_dotenv to read environment variables"


# 3. test that connect_db reads connection string from environment
def test_connect_db_reads_connection_string(monkeypatch):
    getenv_mock = Mock(return_value="postgresql://test")
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", getenv_mock)
    monkeypatch.setattr(database.sa, "create_engine", Mock(return_value=object()))
    database.connect_db()
    getenv_mock.assert_called_once_with("CS")


# 4. test that connect_db passes connection string to create_engine
def test_connect_db_passes_connection_string(monkeypatch):
    create_engine_mock = Mock(return_value=object())
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="postgresql://cafe"))
    monkeypatch.setattr(database.sa, "create_engine", create_engine_mock)
    database.connect_db()
    create_engine_mock.assert_called_once_with("postgresql://cafe")


# 5. test that connect_db logs error when connection fails
def test_connect_db_logs_error_on_failure(monkeypatch):
    monkeypatch.setattr(database, "load_dotenv", Mock())
    monkeypatch.setattr(database.os, "getenv", Mock(return_value="bad_string"))
    monkeypatch.setattr(database.sa, "create_engine", Mock(side_effect=Exception("boom")))
    error_mock = Mock()
    monkeypatch.setattr(database.logger, "error", error_mock)
    result = database.connect_db()
    assert result is None, "connect_db should return None if the connection fails"
    assert error_mock.called, "connect_db should log an error when connection fails"


# ================= tests for drop_table =================

# 1. test that drop_table executes the drop query
def test_drop_table_executes_query(monkeypatch):
    engine = DummyEngine()
    query_mock = Mock(bindparams=Mock(return_value="DROP_QUERY"))
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=query_mock))
    database.drop_table("sales")
    engine._connection.execute.assert_called_once_with("DROP_QUERY")


# 2. test that drop_table commits the transaction
def test_drop_table_commits_transaction(monkeypatch):
    engine = DummyEngine()
    query_mock = Mock(bindparams=Mock(return_value="DROP_QUERY"))
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=query_mock))
    database.drop_table("sales")
    assert engine._connection.commit.called, "drop_table should commit the database transaction"


# 3. test that drop_table disposes the engine
def test_drop_table_disposes_engine(monkeypatch):
    engine = DummyEngine()
    query_mock = Mock(bindparams=Mock(return_value="DROP_QUERY"))
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=query_mock))
    database.drop_table("sales")
    assert engine.dispose.called, "drop_table should dispose the engine after execution"


# 4. test that drop_table logs errors
def test_drop_table_logs_error(monkeypatch):
    bad_conn = DummyConnection()
    bad_conn.execute.side_effect = Exception("sql error")
    engine = DummyEngine(connection=bad_conn)
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=Mock(bindparams=Mock(return_value="DROP_QUERY"))))
    error_mock = Mock()
    monkeypatch.setattr(database.logger, "error", error_mock)
    database.drop_table("sales")
    assert error_mock.called, "drop_table should log an error if SQL execution fails"


# ================= tests for write_from_dataframe =================

# 1. test that dataframe is written to the database
def test_write_from_dataframe_calls_to_sql(monkeypatch):
    df = pd.DataFrame({"item": ["coffee"]})
    engine = DummyEngine()
    to_sql_mock = Mock()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(pd.DataFrame, "to_sql", to_sql_mock, raising=True)
    database.write_from_dataframe(df, "sales")
    assert to_sql_mock.called, "write_from_dataframe should call dataframe.to_sql"


# 2. test that correct table name is used
def test_write_from_dataframe_uses_table_name(monkeypatch):
    df = pd.DataFrame({"item": ["coffee"]})
    engine = DummyEngine()
    to_sql_mock = Mock()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(pd.DataFrame, "to_sql", to_sql_mock, raising=True)
    database.write_from_dataframe(df, "sales")
    table_name = to_sql_mock.call_args[0][0]
    assert table_name == "sales", "write_from_dataframe should pass the correct table name"


# 3. test warning when dataframe is empty
def test_write_from_dataframe_logs_warning_for_empty_df(monkeypatch):
    df = pd.DataFrame()
    engine = DummyEngine()
    warning_mock = Mock()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.logger, "warning", warning_mock)
    monkeypatch.setattr(pd.DataFrame, "to_sql", Mock(), raising=True)
    database.write_from_dataframe(df, "sales")
    assert warning_mock.called, "write_from_dataframe should log warning for empty dataframe"


# 4. test that write_from_dataframe logs errors
def test_write_from_dataframe_logs_error(monkeypatch):
    df = pd.DataFrame({"item": ["cake"]})
    engine = DummyEngine()
    error_mock = Mock()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(pd.DataFrame, "to_sql", Mock(side_effect=Exception("write failed")), raising=True)
    monkeypatch.setattr(database.logger, "error", error_mock)
    database.write_from_dataframe(df, "sales")
    assert error_mock.called, "write_from_dataframe should log errors if writing fails"


# ================= tests for read_as_dataframe =================

# 1. test that read_as_dataframe returns dataframe
def test_read_as_dataframe_returns_dataframe(monkeypatch):
    engine = DummyEngine()
    expected_df = pd.DataFrame({"item": ["coffee"]})
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=Mock(bindparams=Mock(return_value="SELECT_QUERY"))))
    monkeypatch.setattr(database.pd, "read_sql", Mock(return_value=expected_df))
    result = database.read_as_dataframe("sales")
    assert isinstance(result, pd.DataFrame), "read_as_dataframe should return a pandas dataframe"
    pd.testing.assert_frame_equal(result, expected_df)


# 2. test that read_as_dataframe calls pandas read_sql
def test_read_as_dataframe_calls_read_sql(monkeypatch):
    engine = DummyEngine()
    read_sql_mock = Mock(return_value=pd.DataFrame())
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=Mock(bindparams=Mock(return_value="SELECT_QUERY"))))
    monkeypatch.setattr(database.pd, "read_sql", read_sql_mock)
    database.read_as_dataframe("sales")
    assert read_sql_mock.called, "read_as_dataframe should call pandas.read_sql"


# 3. test that read_as_dataframe logs errors
def test_read_as_dataframe_logs_error(monkeypatch):
    engine = DummyEngine()
    error_mock = Mock()
    monkeypatch.setattr(database, "connect_db", Mock(return_value=engine))
    monkeypatch.setattr(database.sa, "text", Mock(return_value=Mock(bindparams=Mock(return_value="SELECT_QUERY"))))
    monkeypatch.setattr(database.pd, "read_sql", Mock(side_effect=Exception("read failed")))
    monkeypatch.setattr(database.logger, "error", error_mock)
    result = database.read_as_dataframe("sales")
    assert result is None, "read_as_dataframe should return None if reading fails"
    assert error_mock.called, "read_as_dataframe should log errors when read_sql fails"