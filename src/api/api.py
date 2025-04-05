import logging
import os
import traceback

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from manager import MANAGER
from query_cache import QUERY_CACHE
from src.schema.schema import ConfigRequest, DBConfig, DBEngine, Query, QueryError, SupportedOutputFormats
from src.utils.utils import response_to_file

app = FastAPI()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.get("/")
async def read_root():
    return "BGDB is up and running!"


@app.get("/fetch_configs")
async def fetch_configs() -> dict[str, DBConfig]:
    return MANAGER.configs


@app.post("/create_engine")
async def create_engine(config_request: ConfigRequest) -> DBEngine:
    engine = await MANAGER.start_engine(config_request.db_config_id)
    return engine


@app.get("/get_engines")
async def get_engines() -> list[DBEngine]:
    return MANAGER.show_engines()


@app.post("/query")
async def query(q: Query) -> Response:
    try:
        data = await MANAGER.execute_query(q.query, q.engine_id)
    except QueryError as e:
        logger.warning(f"Encountered the following error: {e}")

        logger.warning(traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=400)

    db_config_id = MANAGER.engines[str(q.engine_id)]["db_config_id"]
    cache_storage_format = (
        q.options.file_redirection.output_format if q.options.file_redirection else SupportedOutputFormats.JSON
    )
    query_id, cache_file = QUERY_CACHE.store(q.query, db_config_id, data, cache_storage_format)

    if q.options.file_redirection is None:
        return JSONResponse(content={"query_id": query_id, "data": data})

    data = response_to_file(data, q.options.file_redirection)

    return JSONResponse(
        content={
            "message": "Data redirected",
            "query_id": query_id,
            "file": os.path.abspath(cache_file),
        },
        status_code=200,
    )
