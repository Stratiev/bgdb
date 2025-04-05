import yaml
import json
import os
import uuid
from pathlib import PosixPath
from sqlalchemy import MetaData, inspect

import aiosqlite
from pydantic import UUID4
from sqlalchemy import text, Table
from sqlalchemy.exc import NoSuchTableError, PendingRollbackError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from src.schema.schema import ConfigValidationError, DBConfig, DBResult, DBEngine, QueryError
from src.utils.table_serialization import parse_table
from src.utils.utils import validate_config

DEFAULT_CONFIG_FOLDER = os.path.expandvars("$HOME/Work/git/bgdb")
TABLE_INFO_FILE = "/tmp/table_info.json"


def load_config(conf: str | PosixPath) -> dict[str, DBConfig]:
    with open(conf, "r") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            f.seek(0)
            config = yaml.safe_load(f)
    return {k: validate_config(c) for k, c in config.items()}


def custom_inspect(sync_conn):
    reflector = inspect(sync_conn)
    schema_names = reflector.get_schema_names()
    schema_tables = []
    for schema_name in schema_names:
        schema_tables.extend(
            [
                (schema_name, Table(tn, MetaData(), schema=schema_name))
                for tn in reflector.get_table_names(schema=schema_name)
            ]
        )
    for schema_name, table in schema_tables:
        try:
            reflector.reflect_table(table, None)
        except NoSuchTableError:
            print("Failure")
            print((schema_name, table))
    return [x[1] for x in schema_tables]


async def reflect_schema(engine: AsyncEngine):
    async with engine.connect() as conn:
        # Run synchronous inspect code inside a sync block
        return await conn.run_sync(custom_inspect)


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

    def table_info(self):
        info = {}
        for _, engine_data in self.engines.items():
            parsed_tables = []
            for table in engine_data["tables"]:
                parsed_tables.append(parse_table(table))
            info[engine_data["db_config_id"]] = parsed_tables
        return info

    def store_table_info(self):
        info = self.table_info()
        with open(TABLE_INFO_FILE, "w") as f:
            json.dump(info, f, indent=4)

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
        tables = await reflect_schema(engine)
        self.engines[engine_uuid] = {
            "session_maker": session_maker,
            "engine": engine,
            "db_config_id": db_config_id,
            "db_config": db_config.model_dump(),
            "tables": tables,
        }
        # TODO: Make this so that it updates each time the schema is reflected.
        self.store_table_info()
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
