import csv
import json
from datetime import datetime
from io import StringIO
from uuid import UUID

from pydantic import SecretStr, ValidationError
from src.schema.schema import ConfigValidationError, DBConfig, SupportedOutputFormats, db_config_types


def dicts_to_csv_str(data: list[dict]) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    csv_string = output.getvalue()
    output.close()
    return csv_string


def response_convert(data, output_format):
    # The only advantage of this is to get mypy to stop complaining.
    mapping = {SupportedOutputFormats.JSON: json.dumps, SupportedOutputFormats.CSV: dicts_to_csv_str}
    return mapping[output_format](data)


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
