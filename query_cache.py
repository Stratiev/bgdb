import hashlib
import json
import os
from typing import Optional

CACHE_DIR = "query_cache"
QUERY_LOG_FILE = os.path.join(CACHE_DIR, "queries.json")
os.makedirs(CACHE_DIR, exist_ok=True)


def compute_hash(data: str) -> str:
    """Compute a fixed-length hash of the given data."""
    return hashlib.sha256(data.encode()).hexdigest()


def load_query_log():
    """Load the query log file, creating an empty structure if it doesn't exist."""
    if not os.path.exists(QUERY_LOG_FILE):
        return {}
    with open(QUERY_LOG_FILE, "r") as f:
        return json.load(f)


def save_query_log(query_log):
    """Save the query log file."""
    with open(QUERY_LOG_FILE, "w") as f:
        json.dump(query_log, f, indent=4)


class QueryCache:
    """Handles caching of queries and their results."""

    def __init__(self, cache_dir: str = CACHE_DIR):
        self.cache_dir = cache_dir

    def store(self, query: str, db_config_id: str, result: dict, output_format: Optional[str] = "json") -> str:
        query_hash = compute_hash(query + db_config_id)
        result_ext = "csv" if output_format == "csv" else "json"
        result_content = json.dumps(result, sort_keys=True) if output_format != "csv" else self._convert_to_csv(result)
        result_file = os.path.join(self.cache_dir, f"{db_config_id}_{query_hash}_result.{result_ext}")

        # Load query log
        query_log = load_query_log()

        # Store query only if it doesn't exist
        if db_config_id not in query_log:
            query_log[db_config_id] = []

        if not any(entry["hash"] == query_hash for entry in query_log[db_config_id]):
            query_log[db_config_id].append({"hash": query_hash, "query": query})
            save_query_log(query_log)

        # Store result only if different
        if not os.path.exists(result_file):
            with open(result_file, "w") as f:
                f.write(result_content)

        return query_hash

    def _convert_to_csv(self, data: dict) -> str:
        """Convert result to CSV format."""
        if not isinstance(data, list) or not data:
            return ""
        keys = data[0].keys()
        csv_data = ",".join(keys) + "\n"
        for row in data:
            csv_data += ",".join(str(row[k]) for k in keys) + "\n"
        return csv_data


QUERY_CACHE = QueryCache()
