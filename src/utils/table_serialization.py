import json
from sqlalchemy import Table
from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint


def serialize_sqlalchemy_object(obj):
    """
    Serialize SQLAlchemy specific objects to a JSON-compatible format.
    """
    if isinstance(obj, PrimaryKeyConstraint):
        return {"type": "PrimaryKey", "columns": [col.name for col in obj.columns]}

    if isinstance(obj, ForeignKeyConstraint):
        return {
            "type": "ForeignKey",
            "columns": [col.name for col in obj.columns],
            "references": str(obj.referred_table),
        }

    if isinstance(obj, Table):
        return parse_table(obj)

    # Return a string for other types of SQLAlchemy objects
    return str(obj)


def parse_table(table: Table):
    """
    Parse the table object to extract relevant schema data for JSON serialization.
    """
    table_info = {
        "name": table.name,
        "full_name": f"{table.schema}.{table.name}" if table.schema else table.name,
        "schema": table.schema,
        "description": table.description,
        "columns": [],
        "primary_key": [],
        "foreign_keys": [],
        "constraints": [],
    }

    for column in table.columns:
        column_info = {
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "primary_key": column.primary_key,
            "unique": column.unique,
            "foreign_key": None,
        }

        for fk in column.foreign_keys:
            column_info["foreign_key"] = str(fk)

        table_info["columns"].append(column_info)

    if table.primary_key:
        for pk in table.primary_key.columns:
            table_info["primary_key"].append(pk.name)

    for fk in table.foreign_keys:
        table_info["foreign_keys"].append(
            {"column": fk.parent.name, "referenced_table": fk.column.table.name, "referenced_column": fk.column.name}
        )

    for constraint in table.constraints:
        table_info["constraints"].append(serialize_sqlalchemy_object(constraint))

    return table_info
