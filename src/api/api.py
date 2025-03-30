import logging
import os
import traceback

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from manager import MANAGER
from query_cache import QUERY_CACHE
from src.schema.schema import ConfigRequest, DBConfig, DBSession, Query, QueryError, SupportedOutputFormats
from src.utils.utils import response_convert

app = FastAPI()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


@app.post("/query")
async def query(q: Query) -> Response:
    try:
        data = await MANAGER.execute_query(q.query, q.session_id)
    except QueryError as e:
        logger.warning(f"Encountered the following error: {e}")

        logger.warning(traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=400)

    db_config_id = MANAGER.sessions[str(q.session_id)]["db_config_id"]
    cache_storage_format = (
        q.options.file_redirection.output_format if q.options.file_redirection else SupportedOutputFormats.JSON
    )
    query_id, cache_file = QUERY_CACHE.store(q.query, db_config_id, data, cache_storage_format)

    if q.options.file_redirection is None:
        return JSONResponse(content={"query_id": query_id, "data": data})

    data = response_convert(data, q.options.file_redirection.output_format)
    with open(q.options.file_redirection.output_file, "w") as f:
        f.write(data)

    return JSONResponse(
        content={
            "message": "Data redirected",
            "query_id": query_id,
            "file": os.path.abspath(cache_file),
        },
        status_code=200,
    )
