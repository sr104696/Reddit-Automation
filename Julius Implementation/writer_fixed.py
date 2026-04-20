# Writer utilities with config fallback and filesystem-safe defaults
import json
import os
from pathlib import Path
from typing import Any, Dict

OUTPUT_DIR = Path(os.environ.get('OUTPUT_DIR', './output'))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

try:
    from config import settings
except Exception:
    settings = {}


def write_json(filename: str, payload: Dict[str, Any]) -> str:
    path_obj = OUTPUT_DIR / filename
    with open(path_obj, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return str(path_obj)


def write_text(filename: str, content: str) -> str:
    path_obj = OUTPUT_DIR / filename
    with open(path_obj, 'w', encoding='utf-8') as handle:
        handle.write(content)
    return str(path_obj)
