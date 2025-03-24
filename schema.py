from datetime import datetime
from enum import Enum, auto
from pathlib import PosixPath
from typing import Optional
from uuid import UUID

from pydantic import UUID4, BaseModel, Field, SecretStr
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.result import RMKeyView


class DBConfig(BaseModel):
    hostname: str
    port: int
    dbname: str
    username: str
    password: SecretStr
    protocol: str
    use_ssl: Optional[bool] = None

    @property
    def url(self):
        password = self.password.get_secret_value()
        base_url = f"{self.protocol}://{self.username}:{password}@{self.hostname}:{self.port}/{self.dbname}"
        return f"{base_url}?ssl=require" if self.use_ssl else base_url


class DBSession(BaseModel):
    session_uuid: UUID4
    db_config: DBConfig
    connected_since: datetime

    class Config:
        # Custom serializers for specific fields
        json_encoders = {
            UUID: lambda v: str(v),  # Serialize UUID as string
            datetime: lambda v: v.isoformat(),  # Serialize datetime to ISO format
            SecretStr: lambda v: "***",  # Serialize datetime to ISO format
        }


class SupportedOutputFormats(Enum):
    CSV = auto()
    JSON = auto()


class FileRedirection(BaseModel):
    output_file: PosixPath
    output_format: SupportedOutputFormats = SupportedOutputFormats.CSV


class QueryOptions(BaseModel):
    file_redirection: Optional[FileRedirection] = None


class Query(BaseModel):
    query: str
    session_id: UUID4
    options: QueryOptions = Field(default_factory=QueryOptions)


class DBResult:
    def __init__(self, cursor_result: CursorResult):
        self.columns: RMKeyView = cursor_result.keys()
        self.items = cursor_result.fetchall()

    def to_dicts(self):
        return [dict(zip(self.columns, row)) for row in self.items]
