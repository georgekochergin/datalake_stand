"""Master-only Spark application admission guard, independent of Airflow."""

import json
from contextlib import contextmanager
from http.client import HTTPConnection, HTTPSConnection
import logging
import time
from urllib.parse import urlencode, urlsplit

GUARD_TIMEOUT = 120
HTTP_TIMEOUT = 5
POLL_INTERVAL = 2
LOG = logging.getLogger(__name__)


class SparkGuardError(RuntimeError):
    """Refuse submission when the master cannot prove safe admission."""


def _remaining(deadline):
    """Return the remaining monotonic budget or refuse admission."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SparkGuardError("Spark guard timed out; submit prohibited")
    return remaining


@contextmanager
def _request(method, url, deadline, data=None):
    """Open one bounded HTTP request and always close its connection."""
    request_deadline = min(deadline, time.monotonic() + HTTP_TIMEOUT)
    parsed = urlsplit(url)
    connection_type = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
    connection = connection_type(parsed.hostname, parsed.port, timeout=_remaining(request_deadline))
    try:
        connection.connect()
        connection.sock.settimeout(_remaining(request_deadline))
        headers = {} if data is None else {"Content-Type": "application/x-www-form-urlencoded"}
        connection.request(method, parsed.path, body=data, headers=headers)
        connection.sock.settimeout(_remaining(request_deadline))
        response = connection.getresponse()
        with response:
            yield response
            _remaining(request_deadline)
    except SparkGuardError:
        raise
    except Exception:
        raise SparkGuardError("Spark master HTTP request failed; submit prohibited") from None
    finally:
        connection.close()


def _get_master_state(master, deadline):
    """Fetch and decode the master application inventory through GET /json/."""
    with _request("GET", master + "/json/", deadline) as response:
        if response.status != 200:
            raise SparkGuardError("Spark master state request failed; submit prohibited")
        return json.loads(response.read())


def _kill_application(master, application_id, deadline):
    """POST one kill command without following the UI redirect or reading state."""
    data = urlencode({"id": application_id, "terminate": "true"}).encode("ascii")
    with _request("POST", master + "/app/kill/", deadline, data=data) as response:
        if response.status not in (200, 302, 303):
            raise SparkGuardError("Spark master kill request failed; submit prohibited")


def _applications(snapshot):
    """Validate the complete application inventory before using it as evidence."""
    if not isinstance(snapshot, dict):
        raise SparkGuardError("Invalid Spark master JSON; submit prohibited")
    inventories = {}
    seen = set()
    for key in ("activeapps", "completedapps"):
        applications = snapshot.get(key)
        if not isinstance(applications, list):
            raise SparkGuardError("Missing Spark application inventory; submit prohibited")
        inventory = {}
        for app in applications:
            if not isinstance(app, dict) or any(
                not isinstance(app.get(field), str) or not app[field]
                for field in ("id", "name", "state")
            ):
                raise SparkGuardError("Invalid Spark application record; submit prohibited")
            if app["id"] in seen:
                raise SparkGuardError("Duplicate Spark application ID; submit prohibited")
            seen.add(app["id"])
            inventory[app["id"]] = app
        inventories[key] = inventory
    return inventories["activeapps"], inventories["completedapps"]


def _all_applications_completed(active, completed, original_ids, target_table):
    """Require completion evidence for every original ID and reject new target IDs."""
    current_ids = {key for key, app in active.items() if app["name"] == target_table}
    if current_ids - original_ids:
        raise SparkGuardError("Unexpected new application for target; submit prohibited")
    all_completed = True
    for application_id in original_ids:
        running = active.get(application_id)
        if running is not None:
            if running["name"] != target_table:
                raise SparkGuardError("Unexpected application target; submit prohibited")
            if running["state"] not in ("WAITING", "RUNNING"):
                raise SparkGuardError("Unknown active target state; submit prohibited")
            all_completed = False
            continue
        finished = completed.get(application_id)
        if finished is None:
            raise SparkGuardError("Missing target completion evidence; submit prohibited")
        if finished["name"] != target_table:
            raise SparkGuardError("Unexpected application target; submit prohibited")
        if finished["state"] not in ("KILLED", "FINISHED"):
            raise SparkGuardError("Unknown completed target state; submit prohibited")
    return all_completed


def guard_spark_submit(master, target_table):
    """Kill original target applications once and require master completion evidence."""
    deadline = time.monotonic() + GUARD_TIMEOUT
    active, _ = _applications(_get_master_state(master, deadline))
    original_ids = {key for key, app in active.items() if app["name"] == target_table}
    if not original_ids:
        _remaining(deadline)
        LOG.info("Spark guard target=%s result=admitted application_ids=[]", target_table)
        return []
    if any(active[key]["state"] not in ("WAITING", "RUNNING") for key in original_ids):
        raise SparkGuardError("Unknown active target state; submit prohibited")
    for application_id in sorted(original_ids):
        LOG.info("Spark guard target=%s kill_id=%s", target_table, application_id)
        _kill_application(master, application_id, deadline)
    while True:
        active, completed = _applications(_get_master_state(master, deadline))
        if _all_applications_completed(active, completed, original_ids, target_table):
            break
        time.sleep(min(POLL_INTERVAL, _remaining(deadline)))
    _remaining(deadline)
    LOG.info("Spark guard target=%s result=admitted application_ids=%s", target_table, sorted(original_ids))
    return sorted(original_ids)
