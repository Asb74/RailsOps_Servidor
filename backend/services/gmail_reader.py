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


def _extract_parts_recursive(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []

    for part in payload.get("parts", []) or []:
        if part.get("parts"):
            parts.extend(_extract_parts_recursive(part))

        filename = part.get("filename") or ""
        body = part.get("body") or {}
        has_attachment_id = bool(body.get("attachmentId"))
        has_inline_data = bool(body.get("data"))

        if filename and (has_attachment_id or has_inline_data):
            parts.append(part)

    # Adjuntos no multipart (menos habitual) en el payload raíz
    root_filename = payload.get("filename") or ""
    root_body = payload.get("body") or {}
    if root_filename and (root_body.get("attachmentId") or root_body.get("data")):
        parts.append(payload)

    return parts


def _decode_part_data(service: Any, message_id: str, part: dict[str, Any]) -> bytes:
    body = part.get("body") or {}

    if body.get("attachmentId"):
        attachment = (
            service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=body["attachmentId"])
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


def descargar_adjuntos() -> list[dict[str, Any]]:
    """Descarga todos los adjuntos útiles de correos Gmail con estructura plana.

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

        parts = _extract_parts_recursive(payload)
        if not parts:
            logger.info("[GMAIL] Correo sin adjuntos descargables: gmail_id=%s", msg_id)
            continue

        logger.info("[GMAIL] Correo gmail_id=%s adjuntos_detectados=%s", msg_id, len(parts))

        for idx, part in enumerate(parts, start=1):
            original_name = part.get("filename") or f"adjunto_{idx}"
            filename = _safe_filename(original_name)

            if not _is_useful_attachment(filename):
                logger.info("[GMAIL] Adjunto ignorado por extensión: gmail_id=%s archivo=%s", msg_id, filename)
                continue

            try:
                file_data = _decode_part_data(service, msg_id, part)
                if not file_data:
                    logger.warning("[GMAIL] Adjunto vacío: gmail_id=%s archivo=%s", msg_id, filename)
                    continue

                out_name = f"{msg_id}_{idx}_{filename}"
                file_path = Path(INPUT_FOLDER) / out_name
                file_path.write_bytes(file_data)

                descargados.append(
                    {
                        "filename": out_name,
                        "original_filename": filename,
                        "path": str(file_path),
                        "gmail_id": msg_id,
                        "subject": subject,
                    }
                )
                logger.info("[GMAIL] Adjunto descargado: gmail_id=%s archivo=%s", msg_id, out_name)
            except Exception as exc:  # pragma: no cover - tolerancia red/API
                logger.exception("[GMAIL] Error descargando adjunto gmail_id=%s archivo=%s: %s", msg_id, filename, exc)

    return descargados
