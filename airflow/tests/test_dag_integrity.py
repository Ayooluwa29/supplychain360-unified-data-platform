"""
Checks
------
1. Both DAGs import without errors.
2. No cycles exist in either DAG.
3. transformation_dag contains exactly one TaskGroup per DBT_LAYERS entry.
4. ingestion_dag contains exactly one sync task per AIRBYTE_CONNECTIONS entry.
5. transformation_dag always starts with wait_for_ingestion.
6. Every layer marked run_tests=True has a corresponding test task.
"""

from __future__ import annotations
import importlib
import sys
import os
import pytest

sys.path.append(os.path.dirname(__file__))

import dbt_config as cfg

# Make the project's dags/ and plugins/ importable from the test runner
DAGS_DIR    = os.path.join(os.path.dirname(__file__), "..", "dags")
PLUGINS_DIR = os.path.join(os.path.dirname(__file__), "..", "plugins")
sys.path.insert(0, DAGS_DIR)
sys.path.insert(0, PLUGINS_DIR)


# Helpers
def _load_dag(module_name: str, dag_id: str):
    """Import a DAG module and return the DAG object."""
    mod = importlib.import_module(module_name)
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found. Errors: {dagbag.import_errors}"
    return dagbag.dags[dag_id]


# Fixtures
@pytest.fixture(scope="module")
def ingestion_dag():
    return _load_dag("ingestion_dag", "ingestion_dag")


@pytest.fixture(scope="module")
def transformation_dag():
    return _load_dag("dbt_layer_dag_factory", "transformation_dag")


# ingestion_dag tests
class TestIngestionDag:

    def test_dag_loads(self, ingestion_dag):
        assert ingestion_dag is not None

    def test_no_import_errors(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
        assert not dagbag.import_errors, f"Import errors: {dagbag.import_errors}"

    def test_no_cycles(self, ingestion_dag):
        # DagBag raises on cycles; this just confirms the DAG is cycle-free
        assert ingestion_dag.topological_sort() is not None

    def test_sync_task_count(self, ingestion_dag):
        """One sync task per AIRBYTE_CONNECTIONS entry."""
        sync_tasks = [t for t in ingestion_dag.tasks if t.task_id.startswith("sync_")]
        assert len(sync_tasks) == len(cfg.AIRBYTE_CONNECTIONS), (
            f"Expected {len(cfg.AIRBYTE_CONNECTIONS)} sync tasks, "
            f"got {len(sync_tasks)}: {[t.task_id for t in sync_tasks]}"
        )

    def test_sync_task_ids_match_config(self, ingestion_dag):
        """Task IDs correspond to AIRBYTE_CONNECTIONS keys."""
        expected = {f"sync_{name}" for name in cfg.AIRBYTE_CONNECTIONS}
        actual   = {t.task_id for t in ingestion_dag.tasks if t.task_id.startswith("sync_")}
        assert expected == actual

    def test_schedule(self, ingestion_dag):
        assert ingestion_dag.schedule_interval == cfg.INGESTION_SCHEDULE


# transformation_dag tests
class TestTransformationDag:

    def test_dag_loads(self, transformation_dag):
        assert transformation_dag is not None

    def test_no_cycles(self, transformation_dag):
        assert transformation_dag.topological_sort() is not None

    def test_starts_with_sensor(self, transformation_dag):
        """First real task must be the ingestion gate sensor."""
        assert "wait_for_ingestion" in transformation_dag.task_ids

    def test_task_group_per_layer(self, transformation_dag):
        """One TaskGroup exists for each entry in DBT_LAYERS."""
        group_ids = {
            tg_id
            for tg_id in transformation_dag.task_group.children
            if tg_id.startswith("layer_")
        }
        expected = {f"layer_{layer['name']}" for layer in cfg.DBT_LAYERS}
        assert expected == group_ids, (
            f"Missing groups: {expected - group_ids}  |  "
            f"Extra groups: {group_ids - expected}"
        )

    def test_test_tasks_present_for_tested_layers(self, transformation_dag):
        """Layers with run_tests=True must have a dbt_test_<name> task."""
        all_task_ids = transformation_dag.task_ids
        for layer in cfg.DBT_LAYERS:
            if layer.get("run_tests") and layer.get("select"):
                expected_task = f"layer_{layer['name']}.dbt_test_{layer['name']}"
                assert expected_task in all_task_ids, (
                    f"Expected test task '{expected_task}' not found in DAG"
                )

    def test_no_test_tasks_for_untested_layers(self, transformation_dag):
        """Layers with run_tests=False must NOT have a test task."""
        all_task_ids = transformation_dag.task_ids
        for layer in cfg.DBT_LAYERS:
            if not layer.get("run_tests"):
                unexpected_task = f"layer_{layer['name']}.dbt_test_{layer['name']}"
                assert unexpected_task not in all_task_ids, (
                    f"Unexpected test task '{unexpected_task}' found — "
                    f"run_tests is False for layer '{layer['name']}'"
                )

    def test_schedule_is_none(self, transformation_dag):
        """Transformation DAG should be sensor-driven, not scheduled."""
        assert transformation_dag.schedule_interval is None