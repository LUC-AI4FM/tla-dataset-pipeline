import json
from pathlib import Path
from typing import Any

from jsonschema import ValidationError
from jsonschema import validate as jsonschema_validate


def validate_jsonl(jsonl_path: str, schema_path: str) -> tuple[bool, list[str]]:
    """
    Validate a JSONL file against a JSON schema.

    Args:
        jsonl_path: Path to the JSONL file
        schema_path: Path to the JSON schema file

    Returns:
        A tuple of (success: bool, errors: list[str])
    """
    errors = []

    # Check file existence
    try:
        schema_file = Path(schema_path)
        jsonl_file = Path(jsonl_path)

        if not schema_file.exists():
            return False, [f"Schema file not found: {schema_path}"]
        if not jsonl_file.exists():
            return False, [f"JSONL file not found: {jsonl_path}"]
    except Exception as e:
        return False, [f"File path error: {e}"]

    # Load schema
    try:
        with open(schema_path) as f:
            schema: dict[str, Any] = json.load(f)
    except json.JSONDecodeError as e:
        return False, [f"Invalid JSON in schema file: {e}"]
    except Exception as e:
        return False, [f"Error reading schema file: {e}"]

    # Validate JSONL
    try:
        with open(jsonl_path) as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():  # Skip empty lines
                    continue

                try:
                    record: dict[str, Any] = json.loads(line)
                except json.JSONDecodeError as e:
                    errors.append(f"Line {line_num}: Invalid JSON - {e}")
                    continue

                try:
                    jsonschema_validate(instance=record, schema=schema)
                except ValidationError as e:
                    # Provide detailed error information
                    path = ".".join(str(p) for p in e.absolute_path) or "root"
                    errors.append(f"Line {line_num}: Validation failed at '{path}' - {e.message}")
    except Exception as e:
        return False, [f"Error reading JSONL file: {e}"]

    return len(errors) == 0, errors
