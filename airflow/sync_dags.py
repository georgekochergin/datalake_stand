"""Синхронизация сгенерированных ODS демо-DAG-ов из сервиса sydafa в Airflow."""
import json
import os
import urllib.request

try:
    with urllib.request.urlopen("http://sydafa:8000/integrations/airflow/dags", timeout=15) as r:
        dags = json.loads(r.read().decode()).get("dags", {})
        target_dir = "/opt/airflow/dags/demo"
        os.makedirs(target_dir, exist_ok=True)
        for name, code in dags.items():
            file_path = os.path.join(target_dir, name)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(code)
    print(f"[airflow-init] Successfully synced {len(dags)} demo DAGs from sydafa.")
except Exception as e:
    print(f"[airflow-init] Note: sydafa DAG sync: {e}")
