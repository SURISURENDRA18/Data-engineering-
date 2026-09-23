"""Unit tests for remote Airflow 3 configuration, token authentication, and AirflowClient."""

import os
from unittest.mock import MagicMock, patch

from actives_ingestion.airflow_client import AirflowClient
from actives_ingestion.config import AirflowConfig, PipelineConfig


def test_airflow_config_defaults():
    with patch.dict(os.environ, {}, clear=True):
        cfg = AirflowConfig()
        assert cfg.host == "34.130.209.7"
        assert cfg.port == 32002
        assert cfg.user == ""
        assert cfg.password == ""
        assert cfg.version == "3.3.2"
        assert cfg.ui_url == "http://34.130.209.7:32002"
        assert cfg.api_url == "http://34.130.209.7:32002/api/v2"


def test_pipeline_config_includes_airflow():
    pcfg = PipelineConfig()
    assert hasattr(pcfg, "airflow")
    assert pcfg.airflow.port == 32002


def test_airflow_client_tcp_mock():
    client = AirflowClient()
    with patch("socket.socket") as mock_sock_cls:
        mock_sock = MagicMock()
        mock_sock_cls.return_value = mock_sock
        ok, msg = client.check_tcp_connectivity()
        assert ok is True
        assert "succeeded" in msg
        mock_sock.connect.assert_called_once_with(("34.130.209.7", 32002))


def test_airflow_client_health():
    client = AirflowClient()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"metadatabase": {"status": "healthy"}}

    with patch("requests.get", return_value=mock_resp) as mock_get:
        res = client.check_health()
        assert res["reachable"] is True
        assert res["status_code"] == 200
        assert res["data"]["metadatabase"]["status"] == "healthy"
        mock_get.assert_called_once_with("http://34.130.209.7:32002/api/v2/monitor/health", timeout=10.0)


def test_airflow_client_authenticate_success():
    cfg = AirflowConfig(user="test_user", password="test_password")
    client = AirflowClient(config=cfg)

    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.json.return_value = {
        "access_token": "mock_jwt_token_xyz",
        "token_type": "Bearer",
    }

    with patch("requests.post", return_value=mock_resp) as mock_post:
        ok, msg = client.authenticate()
        assert ok is True
        assert "successful" in msg
        assert client._token == "mock_jwt_token_xyz"
        mock_post.assert_called_once_with(
            "http://34.130.209.7:32002/auth/token",
            json={"username": "test_user", "password": "test_password"},
            timeout=10.0,
        )


def test_airflow_client_authenticate_failure():
    cfg = AirflowConfig(user="bad_user", password="bad_password")
    client = AirflowClient(config=cfg)

    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "Unauthorized credentials"

    with patch("requests.post", return_value=mock_resp):
        ok, msg = client.authenticate()
        assert ok is False
        assert "401" in msg


def test_airflow_client_list_dags_success():
    cfg = AirflowConfig(user="admin", password="password")
    client = AirflowClient(config=cfg)
    client._token = "mock_token"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "dags": [{"dag_id": "actives_ingestion_dag", "is_paused": False}],
        "total_entries": 1,
    }

    with patch("requests.request", return_value=mock_resp) as mock_req:
        res = client.list_dags()
        assert res["success"] is True
        assert res["total_entries"] == 1
        assert res["dags"][0]["dag_id"] == "actives_ingestion_dag"
        mock_req.assert_called_once()
        headers = mock_req.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer mock_token"


def test_airflow_client_get_dag_runs_and_task_instances():
    client = AirflowClient()
    client._token = "mock_token"

    # 1. Test get_dag_runs
    mock_runs_resp = MagicMock()
    mock_runs_resp.status_code = 200
    mock_runs_resp.json.return_value = {
        "dag_runs": [{"dag_run_id": "manual__1", "state": "success"}]
    }

    with patch("requests.request", return_value=mock_runs_resp):
        runs = client.get_dag_runs("actives_ingestion_dag")
        assert runs["success"] is True
        assert len(runs["data"]["dag_runs"]) == 1

    # 2. Test get_task_instances
    mock_tasks_resp = MagicMock()
    mock_tasks_resp.status_code = 200
    mock_tasks_resp.json.return_value = {
        "task_instances": [{"task_id": "validate_and_reject", "state": "success"}]
    }

    with patch("requests.request", return_value=mock_tasks_resp):
        tis = client.get_task_instances("actives_ingestion_dag", "manual__1")
        assert tis["success"] is True
        assert tis["data"]["task_instances"][0]["task_id"] == "validate_and_reject"

    # 3. Test get_task_log
    mock_log_resp = MagicMock()
    mock_log_resp.status_code = 200
    mock_log_resp.headers = {"Content-Type": "application/json"}
    mock_log_resp.json.return_value = {"content": "Task executed successfully"}

    with patch("requests.request", return_value=mock_log_resp):
        log_res = client.get_task_log("actives_ingestion_dag", "manual__1", "validate_and_reject", 1)
        assert log_res["success"] is True
        assert log_res["data"]["content"] == "Task executed successfully"


def test_airflow_client_401_transparent_token_refresh():
    cfg = AirflowConfig(user="admin", password="password")
    client = AirflowClient(config=cfg)
    client._token = "expired_token"

    resp_401 = MagicMock()
    resp_401.status_code = 401

    resp_token = MagicMock()
    resp_token.status_code = 201
    resp_token.json.return_value = {"access_token": "new_refreshed_token", "token_type": "Bearer"}

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {"dags": [], "total_entries": 0}

    with patch("requests.request", side_effect=[resp_401, resp_200]):
        with patch("requests.post", return_value=resp_token):
            res = client.list_dags()
            assert res["success"] is True
            assert client._token == "new_refreshed_token"


def test_airflow_client_trigger_dag():
    client = AirflowClient()
    client._token = "mock_token"
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.json.return_value = {"dag_run_id": "manual__2026-09-22T00:00:00+00:00", "state": "queued"}

    with patch("requests.request", return_value=mock_resp) as mock_req:
        res = client.trigger_dag("actives_ingestion_dag", conf={"test": True})
        assert res["success"] is True
        assert res["data"]["state"] == "queued"
        mock_req.assert_called_once()
