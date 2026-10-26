"""Initialize SimpleAuthManager credentials in the container, then start Airflow."""

import json
import os
from pathlib import Path


def main():
    """Write the runtime password file without embedding credentials in the image."""
    path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
    path.write_text(json.dumps({os.environ["STAND_USER"]: os.environ["STAND_PASSWORD"]}))
    path.chmod(0o600)
    os.execv("/entrypoint", ["/entrypoint", *os.sys.argv[1:]])


if __name__ == "__main__":
    main()
