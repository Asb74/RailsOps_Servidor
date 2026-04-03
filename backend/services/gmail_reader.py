"""Lector de Gmail para ingestión automática de documentos RailOps.

Responsabilidad:
- Consultar correos con adjuntos.
- Recorrer estructuras multipart (incluyendo anidadas).
- Descargar todos los adjuntos útiles en almacenamiento local.
"""

from __future__ import annotations

import base64
import logging
import re
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.config import INPUT_FOLDER

logger = logging.getLogger(__name__)

_ALLOWED_EXTENSIONS = {".pdf", ".xls", ".xlsx", ".csv"}


def get_gmail_service():
    creds = Credentials.from_authorized_user_file("token.json")
    return build("gmail", "v1", credentials=creds)


def clasificar_documento(nombre: str | None) -> str | None:
    nombre_up = (nombre or "").upper()

    if "TBA" in nombre_up:
        return "TBA"
    if "TBP" in nombre_up:
        return "TBP"
    if "HR-" in nombre_up or "HOJA" in nombre_up or "RUTA" in nombre_up or "MALLA" in nombre_up:
        return "MALLA"
    if "CVM" in nombre_up or "VELOCIDAD" in nombre_up:
        return "VELOCIDADES"

    return None


def _safe_filename(name: str) -> str:
    base = Path(name).name
    cleaned = re.sub(r"[^A-Za-z0-9._\- ]+", "_", base).strip()
    return cleaned or "adjunto_sin_nombre"


def _decode_part_data(service: Any, user_id: str, message_id: str, part: dict[str, Any]) -> bytes:
    body = part.get("body") or {}

    if body.get("attachmentId"):
        attachment = (
            service.users()
            .messages()
            .attachments()
            .get(userId=user_id, messageId=message_id, id=body["attachmentId"])
            .execute()
        )
        raw_data = attachment.get("data", "")
    else:
        raw_data = body.get("data", "")

    if not raw_data:
        return b""

    return base64.urlsafe_b64decode(raw_data)


def _is_useful_attachment(filename: str) -> bool:
    suffix = Path(filename).suffix.lower()
    return suffix in _ALLOWED_EXTENSIONS


def _build_unique_output_path(folder: Path, base_name: str) -> Path:
    candidate = folder / base_name
    if not candidate.exists():
        return candidate

    stem = Path(base_name).stem
    suffix = Path(base_name).suffix
    counter = 1
    while True:
        alternative = folder / f"{stem}_{counter}{suffix}"
        if not alternative.exists():
            return alternative
        counter += 1


def extraer_adjuntos(parts, service, user_id, msg_id, archivos):
    """Recorre recursivamente payload.parts y descarga todos los adjuntos."""
    if not parts:
        return

    output_dir = Path(INPUT_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)

    for part in parts:
        subparts = part.get("parts") or []
        if subparts:
            extraer_adjuntos(subparts, service, user_id, msg_id, archivos)

        filename = part.get("filename") or ""
        body = part.get("body") or {}
        attachment_id = body.get("attachmentId")

        if not filename or not attachment_id:
            continue

        safe_name = _safe_filename(filename)
        if not _is_useful_attachment(safe_name):
            logger.info("[GMAIL] Adjunto ignorado por extensión: gmail_id=%s archivo=%s", msg_id, safe_name)
            continue

        try:
            data = _decode_part_data(service, user_id, msg_id, part)
            if not data:
                logger.warning("[GMAIL] Adjunto vacío: gmail_id=%s archivo=%s", msg_id, safe_name)
                continue

            out_path = _build_unique_output_path(output_dir, f"{msg_id}_{safe_name}")
            out_path.write_bytes(data)

            archivos.append(
                {
                    "filename": out_path.name,
                    "original_filename": safe_name,
                    "path": str(out_path),
                    "gmail_id": msg_id,
                }
            )
            print("Descargado:", out_path.name)
            logger.info("[GMAIL] Adjunto descargado: gmail_id=%s archivo=%s", msg_id, out_path.name)
        except Exception as exc:  # pragma: no cover - tolerancia red/API
            logger.exception(
                "[GMAIL] Error descargando adjunto gmail_id=%s archivo=%s: %s",
                msg_id,
                safe_name,
                exc,
            )


def descargar_adjuntos() -> list[dict[str, Any]]:
    """Descarga todos los adjuntos útiles de correos Gmail.

    Retorna una lista de items: {filename, path, gmail_id, subject}.
    """
    service = get_gmail_service()
    messages: list[dict[str, str]] = []
    request = service.users().messages().list(userId="me", q="has:attachment", maxResults=200)

    while request is not None:
        response = request.execute()
        messages.extend(response.get("messages", []))
        request = service.users().messages().list_next(request, response)

    Path(INPUT_FOLDER).mkdir(parents=True, exist_ok=True)
    descargados: list[dict[str, Any]] = []

    for msg in messages:
        msg_id = msg["id"]
        msg_data = service.users().messages().get(userId="me", id=msg_id).execute()
        payload = msg_data.get("payload", {})
        headers = payload.get("headers", []) or []
        subject = next((h.get("value") for h in headers if h.get("name", "").lower() == "subject"), "")

        adjuntos_msg: list[dict[str, Any]] = []
        extraer_adjuntos(payload.get("parts", []), service, "me", msg_id, adjuntos_msg)

        # Compatibilidad para correos con adjunto en el payload raíz sin parts.
        root_filename = payload.get("filename") or ""
        root_body = payload.get("body") or {}
        if root_filename and root_body.get("attachmentId"):
            extraer_adjuntos([payload], service, "me", msg_id, adjuntos_msg)

        if not adjuntos_msg:
            logger.info("[GMAIL] Correo sin adjuntos descargables: gmail_id=%s", msg_id)
            continue

        logger.info("[GMAIL] Correo gmail_id=%s adjuntos_descargados=%s", msg_id, len(adjuntos_msg))

        for item in adjuntos_msg:
            item["subject"] = subject
            descargados.append(item)

    return descargados
