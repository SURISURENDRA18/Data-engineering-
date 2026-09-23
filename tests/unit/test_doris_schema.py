"""Unit tests for Apache Doris schema verification and unique key alignment."""

from unittest.mock import MagicMock

from actives_ingestion.config import DorisConfig
from actives_ingestion.doris_client import DorisClient


def test_stream_load_uses_airflow_connection(monkeypatch):
    """Stream Load uses the deployed Airflow connection, including its HTTP port."""
    response = MagicMock(status_code=200)
    response.json.return_value = {"Status": "Success", "NumberLoadedRows": 1}
    put = MagicMock(return_value=response)
    monkeypatch.setattr("actives_ingestion.doris_client.requests.put", put)
    monkeypatch.setattr(
        "actives_ingestion.doris_client.get_airflow_connection_details",
        lambda _conn_id: {
            "host": "doris.internal",
            "port": 9030,
            "login": "airflow-user",
            "password": "airflow-password",
            "extra": {"http_port": 8040},
        },
    )

    client = DorisClient(config=DorisConfig(database="actives_db", raw_table="raw_actives_daily"))
    assert client._stream_load_http([{"customernumber": "1", "iccid": "2"}]) == 1

    args, kwargs = put.call_args
    assert args[0] == "http://doris.internal:8040/api/actives_db/raw_actives_daily/_stream_load"
    assert kwargs["auth"].username == "airflow-user"


def test_in_memory_doris_client_schema():
    client = DorisClient(in_memory=True)
    keys = client.get_table_unique_keys("raw_actives_daily")
    assert "customernumber" in keys
    assert "iccid" in keys

    schema = client.verify_schema()
    assert schema["all_valid"] is True
    assert schema["tables"]["raw_actives_daily"]["is_valid"] is True


def test_unique_key_parsing_composite():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {
        "Create Table": """
            CREATE TABLE `raw_actives_daily` (
              `customernumber` varchar(64) NOT NULL,
              `iccid` varchar(32) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`customernumber`, `iccid`)
            DISTRIBUTED BY HASH(`iccid`) BUCKETS 8;
        """
    }

    client = DorisClient(connection=mock_conn)
    keys = client.get_table_unique_keys("raw_actives_daily")
    assert keys == ["customernumber", "iccid"]


def test_unique_key_parsing_single_iccid():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = {
        "Create Table": """
            CREATE TABLE `raw_actives_daily` (
              `iccid` varchar(32) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`iccid`)
            DISTRIBUTED BY HASH(`iccid`) BUCKETS 8;
        """
    }

    client = DorisClient(connection=mock_conn)
    keys = client.get_table_unique_keys("raw_actives_daily")
    assert keys == ["iccid"]


def test_verify_schema_detects_key_mismatch():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # 1. SHOW DATABASES
    # 2. SHOW TABLES
    # 3. SHOW CREATE TABLE raw_actives_daily (wrong key)
    # 4. SHOW CREATE TABLE actives_file_tracking (correct key)
    mock_cursor.fetchall.side_effect = [
        [{"Database": "actives_db"}],
        [
            {"Tables_in_actives_db": "raw_actives_daily"},
            {"Tables_in_actives_db": "raw_actives_daily_rejected"},
            {"Tables_in_actives_db": "actives_file_tracking"},
        ],
    ]

    mock_cursor.fetchone.side_effect = [
        # SHOW CREATE TABLE raw_actives_daily
        {"Create Table": "CREATE TABLE raw_actives_daily (iccid varchar(32)) UNIQUE KEY(iccid)"},
        # SHOW CREATE TABLE actives_file_tracking
        {"Create Table": "CREATE TABLE actives_file_tracking (file_hash varchar(64)) UNIQUE KEY(file_hash)"},
    ]

    client = DorisClient(config=DorisConfig(database="actives_db"), connection=mock_conn)
    res = client.verify_schema()

    assert res["database_exists"] is True
    assert res["tables"]["raw_actives_daily"]["exists"] is True
    assert res["tables"]["raw_actives_daily"]["is_valid"] is False  # Missing customernumber in key
    assert res["all_valid"] is False


def test_align_schema_if_needed_refuses_mutation_when_key_is_wrong():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # 1. SHOW TABLES LIKE 'raw_actives_daily' -> exists
    # 2. SHOW CREATE TABLE raw_actives_daily -> wrong key (iccid only)
    # No row-count query is needed: an incorrect key is never auto-migrated.
    mock_cursor.fetchone.side_effect = [
        {"table": "raw_actives_daily"},
        {"Create Table": "CREATE TABLE raw_actives_daily (iccid varchar(32)) UNIQUE KEY(iccid)"},
    ]

    client = DorisClient(config=DorisConfig(database="actives_db"), connection=mock_conn)
    success = client.align_schema_if_needed()
    assert success is False

    # Existing tables must never be dropped or renamed automatically.
    calls = [str(c) for c in mock_cursor.execute.call_args_list]
    assert not any("DROP TABLE" in c or "RENAME" in c for c in calls)
