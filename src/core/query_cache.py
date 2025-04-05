import hashlib
import json
import os

from src.schema.schema import SupportedOutputFormats
from src.utils.utils import dicts_to_csv_str

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


def result_to_content(result: dict, output_format: SupportedOutputFormats) -> str:
    output = ""
    if output_format == SupportedOutputFormats.CSV:
        output = dicts_to_csv_str(result)
    if output_format == SupportedOutputFormats.JSON:
        output = json.dumps(result, sort_keys=True, indent=4)
    return output


class QueryCache:
    """Handles caching of queries and their results."""

    def __init__(self, cache_dir: str = CACHE_DIR):
        self.cache_dir = cache_dir

    def store(
        self, query: str, db_config_id: str, result: dict, output_format: SupportedOutputFormats
    ) -> tuple[str, str]:
        query_hash = compute_hash(query + db_config_id)
        self._update_query_log(query, db_config_id, query_hash)

        result_content = result_to_content(result, output_format)

        # TODO: Store result only if different
        result_file = os.path.join(self.cache_dir, f"{db_config_id}_{query_hash}_result.{output_format.value.lower()}")
        with open(result_file, "w") as f:
            f.write(result_content)

        return query_hash, result_file

    def _update_query_log(self, query: str, db_config_id: str, query_hash: str) -> None:
        # Load query log
        query_log = load_query_log()

        # Store query only if it doesn't exist
        if db_config_id not in query_log:
            query_log[db_config_id] = []

        if not any(entry["hash"] == query_hash for entry in query_log[db_config_id]):
            query_log[db_config_id].append({"hash": query_hash, "query": query})
            save_query_log(query_log)


QUERY_CACHE = QueryCache()
