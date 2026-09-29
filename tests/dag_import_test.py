# tests/test_dag_imports.py
"""Fail if any DAG in dags/ has an import error."""

from airflow.models import DagBag


def test_dag_imports():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)

    if dag_bag.import_errors:
        for filename, error in dag_bag.import_errors.items():
            print(f"--- {filename} ---")
            print(error)
        raise SystemExit(f"{len(dag_bag.import_errors)} DAG(s) failed to import")

    print(f"{len(dag_bag.dags)} DAG(s) imported cleanly: {list(dag_bag.dags.keys())}")


if __name__ == "__main__":
    test_dag_imports()