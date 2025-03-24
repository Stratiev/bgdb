import json
import uuid
from datetime import datetime, timezone
from pathlib import PosixPath

from pydantic import UUID4
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from schema import DBConfig, DBResult, DBSession


def load_config(conf: str | PosixPath) -> DBConfig:
    with open(conf, "r") as f:
        config = json.load(f)
    return DBConfig.model_validate(config)


class SessionManager:
    def __init__(self):
        self.sessions = {}

    async def start_session(self, db_config: DBConfig) -> str:
        session_uuid = str(uuid.uuid4())
        db_url = db_config.url
        engine = create_async_engine(db_url, echo=False)
        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        session = session_maker()
        self.sessions[session_uuid] = {
            "session": session,
            "engine": engine,
            "db_config": db_config.model_dump(),
            "connected_since": datetime.now(timezone.utc),
        }
        return session_uuid

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
                DBSession(session_uuid=uuid, db_config=info["db_config"], connected_since=info["connected_since"])
                for uuid, info in self.sessions.items()
            ]

    async def delete_session(self, session_uuid):
        session_info = self.sessions.pop(session_uuid, None)
        if session_info:
            await session_info["session"].close()
            await session_info["engine"].dispose()
