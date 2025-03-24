import json
from typing import Optional

import uvicorn
from fastapi import FastAPI

from manager import SessionManager, load_config
from schema import DBConfig, DBSession, Query
from utils import response_convert

app = FastAPI()

MANAGER = SessionManager()


@app.get("/")
async def read_root():
    return "BGDB is up and running!"


@app.post("/create_session")
async def create_session(db_config: DBConfig) -> str:
    session_id = await MANAGER.start_session(db_config)
    return session_id


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
