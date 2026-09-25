"""Unit test for Airflow DAG structure and import validation."""

import sys
from unittest.mock import MagicMock


def test_dag_import_and_callables():
    """Verify DAG module can be imported without syntax or import errors."""
    import dags.actives_ingestion_dag as dag_module

    assert hasattr(dag_module, "task_validate_and_reject")
    assert hasattr(dag_module, "task_load_raw_table")
    assert hasattr(dag_module, "default_args")
    assert dag_module.default_args.get("owner") == "data_engineering"
    assert dag_module.default_args.get("retries") == 2

    import dags.surendra.actives_ingestion_dag as surendra_dag_module
    assert hasattr(surendra_dag_module, "task_validate_and_reject")
    assert hasattr(surendra_dag_module, "task_load_raw_table")
    assert hasattr(surendra_dag_module, "default_args")
    assert surendra_dag_module.default_args.get("owner") == "surendra"
    assert surendra_dag_module.default_args.get("retries") == 2



def test_real_airflow_dagbag_validation():
    """Validate DAG structure directly using real Airflow DagBag when installed."""
    import pytest
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Real Apache Airflow is not installed in this environment")

    dagbag = DagBag(dag_folder="dags/actives_ingestion_dag.py")
    assert len(dagbag.import_errors) == 0, f"Airflow import errors: {dagbag.import_errors}"
    dag = dagbag.dags.get("actives_ingestion_dag")
    assert dag is not None, "actives_ingestion_dag was not found in DagBag"
    tasks = {t.task_id: t for t in dag.tasks}
    assert "validate_and_reject" in tasks
    assert "load_raw_table" in tasks
    val_task = tasks["validate_and_reject"]
    downstream_ids = [t.task_id for t in val_task.downstream_list]
    assert "load_raw_table" in downstream_ids


def test_dag_definition_with_mock_airflow():
    """Verify DAG structure, task IDs, and dependencies when Airflow is present."""
    mock_airflow = MagicMock()
    mock_operators = MagicMock()
    mock_python_operator = MagicMock()

    class MockDAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.kwargs = kwargs
            self.tasks = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class MockPythonOperator:
        def __init__(self, task_id, python_callable, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.downstream = []

        def __rshift__(self, other):
            self.downstream.append(other)
            return other

    mock_airflow.DAG = MockDAG
    mock_python_operator.PythonOperator = MockPythonOperator

    with (
        unittest_mock_modules({
            "airflow": mock_airflow,
            "airflow.operators": mock_operators,
            "airflow.operators.python": mock_python_operator,
            "airflow.providers.standard.operators.python": mock_python_operator,
        })
    ):
        if "dags.actives_ingestion_dag" in sys.modules:
            del sys.modules["dags.actives_ingestion_dag"]

        import dags.actives_ingestion_dag as loaded_dag

        assert loaded_dag.dag is not None
        assert loaded_dag.dag.dag_id == "actives_ingestion_dag"
        assert loaded_dag.validate_and_reject.task_id == "validate_and_reject"
        assert loaded_dag.load_raw_table.task_id == "load_raw_table"
        assert loaded_dag.load_raw_table in loaded_dag.validate_and_reject.downstream


class unittest_mock_modules:
    def __init__(self, modules_dict):
        self.modules = modules_dict
        self.orig_modules = {}

    def __enter__(self):
        for name, mod in self.modules.items():
            self.orig_modules[name] = sys.modules.get(name)
            sys.modules[name] = mod

    def __exit__(self, exc_type, exc_val, exc_tb):
        for name, orig in self.orig_modules.items():
            if orig is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = orig
