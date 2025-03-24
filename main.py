import json
from typing import Optional

import uvicorn
from fastapi import FastAPI

from manager import SessionManager, load_config
from schema import DBSession, Query
from utils import custom_serializer, response_convert

app = FastAPI()

MANAGER = SessionManager()


@app.get("/")
async def read_root():
    return "BGDB is up and running!"


@app.get("/load_default_config")
async def load_default_config() -> str:
    db_config = load_config("config.json")
    session_id = await MANAGER.start_session(db_config)
    print("Session started:", session_id)
    print("Sessions:", MANAGER.show_sessions())
    return session_id


@app.get("/get_sessions")
async def get_sessions() -> list[DBSession]:
    return MANAGER.show_sessions()


# return json.dumps(sessions, default=custom_serializer)


@app.get("/delete_session/{session_id}")
async def delete_session(session_id: str) -> str:
    try:
        await MANAGER.delete_session(session_id)
    except Exception as e:
        return f"Failed to delete due to error {e}"
    return f"Successfully deleted session {session_id}"


@app.post("/query")
async def query(q: Query) -> Optional[str]:
    data = await MANAGER.execute_query(q.query, q.session_id)
    if q.options.file_redirection is None:
        return json.dumps(data)
    data = response_convert(data, q.options.file_redirection.output_format)
    breakpoint()
    with open(q.options.file_redirection.output_file, "w") as f:
        f.write(data)


if __name__ == "__main__":
    uvicorn.run("main:app", port=8948, log_level="info")
