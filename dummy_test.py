import json
from pathlib import PosixPath

import requests

from schema import FileRedirection, Query, QueryOptions, SupportedOutputFormats

url = "http://127.0.0.1:8948"

print(requests.get(f"{url}/").text)
print(requests.get(f"{url}/get_sessions").text)
sessions = json.loads(requests.get(f"{url}/get_sessions").text)
session_id = requests.get(f"{url}/load_default_config").text.strip('"')
print(session_id)
print(requests.get(f"{url}/get_sessions").text)
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

fr = FileRedirection(output_file=PosixPath("/tmp/dfgrth.csv"))
opts = QueryOptions(file_redirection=fr)

payload = Query(query=query, session_id=session_id, options=opts)

headers = {"Content-Type": "application/json"}
breakpoint()

output = requests.post(f"{url}/query", headers=headers, data=payload.model_dump_json())
