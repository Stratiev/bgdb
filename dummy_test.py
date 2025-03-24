import json
from pathlib import PosixPath

import requests
from pydantic import SecretStr

from manager import load_config
from schema import FileRedirection, Query, QueryOptions

headers = {"Content-Type": "application/json"}
url = "http://127.0.0.1:8948"

print(requests.get(f"{url}/").text)
print(requests.get(f"{url}/get_sessions").text)
sessions = json.loads(requests.get(f"{url}/get_sessions").text)


configs = json.loads(requests.get(f"{url}/fetch_configs").text)
configs.keys()
breakpoint()
config_data = json.dumps(
    {
        k: v.get_secret_value() if isinstance(v, SecretStr) else v
        for k, v in load_config("config.json").model_dump().items()
    }
)
output = requests.post(f"{url}/create_session", headers=headers, data=config_data)
{"db_config_id": "config_1"}
output = requests.post(f"{url}/create_session", headers=headers, json={"db_config_id": "config_1"})
session_id = requests.get(f"{url}/load_default_config").text.strip('"')
print(session_id)
print(requests.get(f"{url}/get_sessions").text)
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

fr = FileRedirection(output_file=PosixPath("/tmp/dfgrth.csv"))
opts = QueryOptions(file_redirection=fr)

payload = Query(query=query, session_id=session_id, options=opts)

breakpoint()

output = requests.post(f"{url}/query", headers=headers, data=payload.model_dump_json())
