"""Client for interacting with the remote Apache Airflow 3 instance.

Provides connectivity testing, health checks, DAG listing, status tracking,
and log retrieval via the Airflow 3 REST API (/api/v2) with OAuth2/JWT Bearer Token Authentication.
"""

import socket
from typing import Any, Optional

import requests

from actives_ingestion.config import AirflowConfig, pipeline_config
from actives_ingestion.utils import get_logger

logger = get_logger("airflow_client")


class AirflowClient:
    """Client for remote Apache Airflow 3 instance."""

    def __init__(self, config: Optional[AirflowConfig] = None, timeout: float = 10.0):
        self.config = config or pipeline_config.airflow
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_type: str = "Bearer"

    def check_tcp_connectivity(self, timeout: Optional[float] = None) -> tuple[bool, str]:
        """Verify raw TCP socket connectivity to the Airflow host:port."""
        t = timeout or self.timeout
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(t)
        try:
            s.connect((self.config.host, self.config.port))
            s.close()
            return True, f"TCP connection to {self.config.host}:{self.config.port} succeeded."
        except Exception as exc:
            s.close()
            return False, f"TCP connection to {self.config.host}:{self.config.port} failed: {exc}"

    def authenticate(self) -> tuple[bool, str]:
        """Authenticate against Airflow 3 POST /auth/token endpoint and acquire JWT access token."""
        if not self.config.user or not self.config.password:
            return False, "Airflow credentials (user or password) not configured"

        url = f"{self.config.ui_url.rstrip('/')}/auth/token"
        payload = {
            "username": self.config.user,
            "password": self.config.password,
        }
        try:
            resp = requests.post(url, json=payload, timeout=self.timeout)
            if resp.status_code in (200, 201):
                data = resp.json()
                self._token = data.get("access_token")
                self._token_type = data.get("token_type", "Bearer")
                return True, "Airflow 3 authentication successful."
            return False, f"Authentication failed with status {resp.status_code}: {resp.text}"
        except Exception as exc:
            return False, f"Authentication request error: {exc}"

    def _get_headers(self) -> dict[str, str]:
        """Build request headers with JWT Bearer token."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if not self._token and self.config.user and self.config.password:
            self.authenticate()

        if self._token:
            headers["Authorization"] = f"{self._token_type} {self._token}"
        return headers

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Execute HTTP request with automatic token refresh on 401."""
        headers = kwargs.pop("headers", None) or self._get_headers()
        timeout = kwargs.pop("timeout", self.timeout)

        resp = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
        if resp.status_code == 401 and self.config.user and self.config.password:
            # Token may have expired; attempt re-authentication once
            ok, _ = self.authenticate()
            if ok:
                headers = self._get_headers()
                resp = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
        return resp

    def check_health(self) -> dict[str, Any]:
        """Query the Airflow 3 health endpoint (/api/v2/monitor/health)."""
        url = f"{self.config.ui_url.rstrip('/')}/api/v2/monitor/health"
        try:
            resp = requests.get(url, timeout=self.timeout)
            return {
                "status_code": resp.status_code,
                "reachable": resp.status_code == 200,
                "data": resp.json() if resp.status_code == 200 else resp.text,
            }
        except Exception as exc:
            return {"status_code": None, "reachable": False, "error": str(exc)}

    def list_dags(self, limit: int = 100) -> dict[str, Any]:
        """Fetch registered DAGs from the Airflow 3 REST API (/api/v2/dags)."""
        url = f"{self.config.api_url.rstrip('/')}/dags"
        try:
            resp = self._request("GET", url, params={"limit": limit})
            if resp.status_code == 200:
                data = resp.json()
                dags = [
                    {"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused")}
                    for d in data.get("dags", [])
                ]
                return {
                    "success": True,
                    "total_entries": data.get("total_entries", len(dags)),
                    "dags": dags,
                }
            return {
                "success": False,
                "status_code": resp.status_code,
                "error": resp.text,
            }
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        """Fetch details for a specific DAG from the Airflow 3 REST API."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}"
        try:
            resp = self._request("GET", url)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_dag_runs(self, dag_id: str, limit: int = 25) -> dict[str, Any]:
        """Fetch DAG runs for a specific DAG from Airflow 3."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns"
        try:
            resp = self._request("GET", url, params={"limit": limit})
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Get the state of a specific DAG run."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns/{dag_run_id}"
        try:
            resp = self._request("GET", url)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        """Fetch task instances for a specific DAG run."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        try:
            resp = self._request("GET", url)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> dict[str, Any]:
        """Fetch a specific task instance state."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        try:
            resp = self._request("GET", url)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def get_task_log(
        self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1
    ) -> dict[str, Any]:
        """Retrieve task execution log for a specific task attempt in Airflow 3."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        try:
            resp = self._request("GET", url)
            if resp.status_code == 200:
                return {"success": True, "data": resp.json() if "application/json" in resp.headers.get("Content-Type", "") else resp.text}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}

    def trigger_dag(self, dag_id: str, conf: Optional[dict] = None) -> dict[str, Any]:
        """Trigger a DAG run on the remote Airflow instance (manual invocation only)."""
        url = f"{self.config.api_url.rstrip('/')}/dags/{dag_id}/dagRuns"
        payload: dict[str, Any] = {"conf": conf or {}}
        try:
            resp = self._request("POST", url, json=payload)
            if resp.status_code in (200, 201):
                return {"success": True, "data": resp.json()}
            return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as exc:
            return {"success": False, "status_code": None, "error": str(exc)}
