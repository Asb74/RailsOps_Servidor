"""Control local de idempotencia para ingestión Gmail.

Responsabilidad: evitar reprocesar correos ya tratados, usando un archivo JSON local.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from backend.config import CONTROL_FILE

logger = logging.getLogger(__name__)


def _control_path() -> Path:
    path = Path(CONTROL_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def cargar_control() -> dict[str, Any]:
    path = _control_path()
    if not path.exists():
        return {"gmail_ids": []}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("[CONTROL] JSON corrupto en %s. Reiniciando control local.", path)
        return {"gmail_ids": []}


def guardar_control(data: dict[str, Any]) -> None:
    path = _control_path()
    clean = {"gmail_ids": sorted(set(data.get("gmail_ids", [])))}

    with NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tmp:
        json.dump(clean, tmp, indent=2, ensure_ascii=False)
        tmp_path = Path(tmp.name)

    tmp_path.replace(path)


def ya_procesado(gmail_id: str) -> bool:
    data = cargar_control()
    return gmail_id in set(data.get("gmail_ids", []))


def marcar_procesado(gmail_id: str) -> None:
    data = cargar_control()
    ids = set(data.get("gmail_ids", []))
    if gmail_id in ids:
        return

    ids.add(gmail_id)
    guardar_control({"gmail_ids": list(ids)})
    logger.info("[CONTROL] gmail_id marcado como procesado: %s", gmail_id)
