import json
from typing import Optional

from fastapi import FastAPI

from manager import SessionManager
from schema import ConfigRequest, DBConfig, DBSession, DefaultEncoder, Query
from utils import response_convert

app = FastAPI()


MANAGER = SessionManager()


@app.get("/")
async def read_root():
    return "BGDB is up and running!"


@app.get("/fetch_configs")
async def fetch_configs() -> dict[str, DBConfig]:
    return MANAGER.configs


@app.post("/create_session")
async def create_session(config_request: ConfigRequest) -> DBSession:
    sessions = await MANAGER.start_session(config_request.db_config_id)
    return sessions


@app.get("/get_sessions")
async def get_sessions() -> list[DBSession]:
    return MANAGER.show_sessions()


@app.get("/delete_session/{session_id}")
async def delete_session(session_id: str) -> str:
    try:
        await MANAGER.delete_session(session_id)
    except Exception as e:
        return f"Failed to delete due to error {e}"
    return f"Successfully deleted session {session_id}"


# TODO: think about what are the correct return types here.
@app.post("/query")
async def query(q: Query) -> str:
    data = await MANAGER.execute_query(q.query, q.session_id)
    if q.options.file_redirection is None:
        print(data)
        return json.dumps(data, cls=DefaultEncoder)
    data = response_convert(data, q.options.file_redirection.output_format)
    with open(q.options.file_redirection.output_file, "w") as f:
        f.write(data)
    return "Data redirected"
