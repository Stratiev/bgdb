import yaml
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import PosixPath

import aiosqlite
from pydantic import UUID4
from sqlalchemy import text
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from src.schema.schema import ConfigValidationError, DBConfig, DBResult, DBEngine, QueryError
from src.utils.utils import validate_config

DEFAULT_CONFIG_FOLDER = os.path.expandvars("$HOME/Work/git/bgdb")


def load_config(conf: str | PosixPath) -> dict[str, DBConfig]:
    with open(conf, "r") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            f.seek(0)
            config = yaml.safe_load(f)
    return {k: validate_config(c) for k, c in config.items()}


def create_engine_and_session_maker(db_url):
    """
    Place db specific stuff in here.
    """
    engine = create_async_engine(db_url, echo=False)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    return engine, session_maker


class EngineManager:
    def __init__(self, config_folder=None):
        self.engines = {}
        self.config_folder = DEFAULT_CONFIG_FOLDER if config_folder is None else config_folder
        self._configs = {}

    @property
    def configs(self) -> dict[str, DBConfig]:
        config_file = f"{self.config_folder}/config.yaml"
        try:
            self._configs = load_config(config_file)
        except ConfigValidationError as e:
            print(f"The config file {config_file} is broken: {e}.")
        return self._configs

    async def start_engine(self, db_config_id: str) -> DBEngine:
        db_config = self.configs[db_config_id]
        existing_engines = {k: v for k, v in self.engines.items() if v.get("db_config_id") == db_config_id}
        if existing_engines:
            engine_uuid = list(existing_engines.keys())[0]
            return DBEngine(
                engine_uuid=uuid.UUID(engine_uuid),
                db_config_id=db_config_id,
                db_config=db_config,
            )

        engine_uuid = str(uuid.uuid4())
        db_url = db_config.url
        engine, session_maker = create_engine_and_session_maker(db_url)
        connected_since = datetime.now(timezone.utc)
        self.engines[engine_uuid] = {
            "session_maker": session_maker,
            "engine": engine,
            "db_config_id": db_config_id,
            "db_config": db_config.model_dump(),
            "connected_since": connected_since,
        }
        return DBEngine(
            engine_uuid=uuid.UUID(engine_uuid),
            db_config_id=db_config_id,
            db_config=db_config,
        )

    async def execute_query(self, query: str, session_uuid: UUID4) -> list[dict]:
        session_info = self.engines.get(str(session_uuid))
        if not session_info:
            raise QueryError("Session not found")
        session = session_info["session_maker"]()
        try:
            result = await session.execute(text(query))
            output = DBResult(result)
            return output.to_dicts()
        except SQLAlchemyError as e:
            if isinstance(e, PendingRollbackError):
                await session.rollback()  # Rollback the invalid transaction
            raise QueryError(e._message())
        finally:
            await session.close()

    def show_engines(self) -> list[DBEngine]:
        if not self.engines:
            return []
        else:
            return [
                DBEngine(
                    engine_uuid=uuid,
                    db_config=info["db_config"],
                    db_config_id=info["db_config_id"],
                )
                for uuid, info in self.engines.items()
            ]


MANAGER = EngineManager()
