"""Lector de Gmail.
Responsabilidad: autenticarse, clasificar y descargar adjuntos operativos.
"""

import base64
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.config import INPUT_FOLDER


def get_gmail_service():
    creds = Credentials.from_authorized_user_file("token.json")
    service = build("gmail", "v1", credentials=creds)
    return service


def clasificar_documento(nombre):
    nombre = nombre.upper()

    if "TBA" in nombre:
        return "TBA"
    if "TBP" in nombre:
        return "TBP"
    if "HR-" in nombre or "HOJA" in nombre or "RUTA" in nombre:
        return "MALLA"
    if "CVM" in nombre or "VELOCIDAD" in nombre:
        return "VELOCIDADES"

    return None


def descargar_adjuntos():
    service = get_gmail_service()

    messages = []
    request = service.users().messages().list(userId="me", q="has:attachment")

    while request is not None:
        response = request.execute()
        messages.extend(response.get("messages", []))
        request = service.users().messages().list_next(request, response)

    archivos = []
    Path(INPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        parts = msg_data.get("payload", {}).get("parts", [])
        if not parts:
            continue

        for part in parts:
            if not part.get("filename"):
                continue
            if "attachmentId" not in part.get("body", {}):
                continue

            attachment_id = part["body"]["attachmentId"]
            attachment = (
                service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=msg["id"], id=attachment_id)
                .execute()
            )

            file_data = base64.urlsafe_b64decode(attachment["data"])
            file_path = Path(INPUT_FOLDER) / part["filename"]
            file_path.write_bytes(file_data)

            archivos.append({"filename": part["filename"], "gmail_id": msg["id"]})

    return archivos
