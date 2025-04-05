import pandas as pd
import json
from datetime import datetime
from uuid import UUID

from pydantic import SecretStr, ValidationError
from src.schema.schema import ConfigValidationError, DBConfig, FileRedirection, SupportedOutputFormats, db_config_types


def dicts_to_csv_str(data: list[dict]) -> str:
    return pd.DataFrame(data).to_csv()


def response_to_file(data, file_redirection: FileRedirection):
    if file_redirection.output_format == SupportedOutputFormats.JSON:
        with open(file_redirection.output_file, "w") as f:
            json.dump(data, f)
    if file_redirection.output_format == SupportedOutputFormats.CSV:
        pd.DataFrame(data).to_csv(file_redirection.output_file)


# Custom serializer for UUID
def custom_serializer(obj):
    if isinstance(obj, UUID):
        return str(obj)  # Convert UUID to string
    elif isinstance(obj, SecretStr):
        return "***"  # Mask the SecretStr for security
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 string
    raise TypeError(f"Type {type(obj)} not serializable")


def validate_config(config: dict) -> DBConfig:
    for config_type in db_config_types:
        try:
            return config_type.model_validate(config)
        except ValidationError:
            continue
    raise ConfigValidationError(f"Invalid config type: {config}")
