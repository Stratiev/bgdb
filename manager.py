import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import PosixPath

from pydantic import UUID4
from schema import DBConfig, DBResult, DBSession
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DEFAULT_CONFIG_FOLDER = os.path.expandvars("$HOME/Work/git/bgdb")


def load_config(conf: str | PosixPath) -> dict[str, DBConfig]:
    with open(conf, "r") as f:
        config = json.load(f)
    return {k: DBConfig.model_validate(c) for k, c in config.items()}


class SessionManager:
    def __init__(self, config_folder=None):
        self.sessions = {}
        self.config_folder = DEFAULT_CONFIG_FOLDER if config_folder is None else config_folder
        self._configs = {}

    @property
    def configs(self) -> dict[str, DBConfig]:
        config_file = f"{self.config_folder}/config.json"
        try:
            self._configs = load_config(config_file)
        except Exception:
            print(f"The config file is broken: {config_file}.")
        return self._configs

    async def start_session(self, db_config_id: str) -> DBSession:
        db_config = self.configs[db_config_id]
        existing_session = {k: v for k, v in self.sessions.items() if v.get("db_config_id") == db_config_id}
        if existing_session:
            session_uuid = list(existing_session.keys())[0]
            return DBSession(
                session_uuid=uuid.UUID(session_uuid),
                db_config_id=db_config_id,
                db_config=db_config,
                connected_since=existing_session[session_uuid]["connected_since"],
            )

        session_uuid = str(uuid.uuid4())
        db_url = db_config.url
        engine = create_async_engine(db_url, echo=False)
        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        session = session_maker()
        connected_since = datetime.now(timezone.utc)
        self.sessions[session_uuid] = {
            "session": session,
            "engine": engine,
            "db_config_id": db_config_id,
            "db_config": db_config.model_dump(),
            "connected_since": connected_since,
        }
        return DBSession(
            session_uuid=uuid.UUID(session_uuid),
            db_config_id=db_config_id,
            db_config=db_config,
            connected_since=connected_since,
        )

    async def execute_query(self, query: str, session_uuid: UUID4) -> list[dict]:
        session_info = self.sessions.get(str(session_uuid))
        if not session_info:
            return "Session not found"
        session = session_info["session"]
        try:
            result = await session.execute(text(query))
            output = DBResult(result)
            return output.to_dicts()
        except SQLAlchemyError as e:
            return str(e)

    def show_sessions(self) -> list[DBSession]:
        if not self.sessions:
            return []
        else:
            return [
                DBSession(
                    session_uuid=uuid,
                    db_config=info["db_config"],
                    connected_since=info["connected_since"],
                    db_config_id=info["db_config_id"],
                )
                for uuid, info in self.sessions.items()
            ]

    async def delete_session(self, session_uuid):
        session_info = self.sessions.pop(session_uuid, None)
        if session_info:
            await session_info["session"].close()
            await session_info["engine"].dispose()
