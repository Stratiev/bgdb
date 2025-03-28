import json
from datetime import datetime
from enum import Enum, auto
from pathlib import PosixPath
from typing import Literal, Optional, Union
from uuid import UUID

from pydantic import UUID4, BaseModel, Field, SecretStr, ValidationError
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.result import RMKeyView

default_encoder = {
    UUID: lambda v: str(v),  # Serialize UUID as string
    datetime: lambda v: v.isoformat(),  # Serialize datetime to ISO format
    SecretStr: lambda v: "***",  # Serialize datetime to ISO format
}


class DefaultEncoder(json.JSONEncoder):
    def default(self, o):
        for data_type, serialization in default_encoder.items():
            if isinstance(o, data_type):
                return serialization(o)
        return json.JSONEncoder.default(self, o)


class PsqlConfig(BaseModel):
    hostname: str
    port: int
    dbname: str
    username: str
    password: SecretStr
    protocol: Literal["postgresql+asyncpg"]
    use_ssl: Optional[bool] = None

    @property
    def url(self):
        password = self.password.get_secret_value()
        base_url = f"{self.protocol}://{self.username}:{password}@{self.hostname}:{self.port}/{self.dbname}"
        return f"{base_url}?ssl=require" if self.use_ssl else base_url


class SqlliteConfig(BaseModel):
    path: str
    protocol: Literal["sqlite+aiosqlite"]

    @property
    def url(self):
        return f"{self.protocol}:///{self.path}"


# This can't be dynamic so repetition is necessary here.
DBConfig = Union[PsqlConfig, SqlliteConfig]
db_config_types = [PsqlConfig, SqlliteConfig]


class ConfigValidationError(Exception):
    pass


class DBSession(BaseModel):
    session_uuid: UUID4
    db_config_id: str
    db_config: DBConfig
    connected_since: datetime

    class Config:
        # Custom serializers for specific fields
        json_encoders = default_encoder


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


class ConfigRequest(BaseModel):
    db_config_id: str


class DBResult:
    def __init__(self, cursor_result: CursorResult):
        self.columns: RMKeyView = cursor_result.keys()
        self.items = cursor_result.fetchall()

    def to_dicts(self):
        return [dict(zip(self.columns, row)) for row in self.items]
