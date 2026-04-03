import os
import base64

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config import INPUT_FOLDER


def get_gmail_service():
    creds = Credentials.from_authorized_user_file("token.json")
    service = build('gmail', 'v1', credentials=creds)
    return service


# 🔥 CLASIFICACIÓN INTELIGENTE
def clasificar_documento(nombre):
    nombre = nombre.upper()

    if "TBA" in nombre:
        return "TBA"

    if "TBP" in nombre:
        return "TBP"

    # HOJAS DE RUTA
    if "HR-" in nombre or "HOJA" in nombre or "RUTA" in nombre:
        return "MALLA"

    # VELOCIDADES
    if "CVM" in nombre or "VELOCIDAD" in nombre:
        return "VELOCIDADES"

    return None  # 🔥 IMPORTANTE


# 🔥 DESCARGA COMPLETA (CON PAGINACIÓN)
def descargar_adjuntos():
    service = get_gmail_service()

    messages = []
    request = service.users().messages().list(userId='me', q='has:attachment')

    while request is not None:
        response = request.execute()
        messages.extend(response.get('messages', []))
        request = service.users().messages().list_next(request, response)

    archivos = []

    for msg in messages:

        msg_data = service.users().messages().get(
            userId='me',
            id=msg['id']
        ).execute()

        parts = msg_data.get('payload', {}).get('parts', [])

        if not parts:
            continue

        for part in parts:
            if part.get('filename'):

                if 'attachmentId' not in part.get('body', {}):
                    continue

                attachment_id = part['body']['attachmentId']

                attachment = service.users().messages().attachments().get(
                    userId='me',
                    messageId=msg['id'],
                    id=attachment_id
                ).execute()

                file_data = base64.urlsafe_b64decode(attachment['data'])

                file_path = os.path.join(INPUT_FOLDER, part['filename'])

                with open(file_path, 'wb') as f:
                    f.write(file_data)

                archivos.append({
                    "filename": part['filename'],
                    "gmail_id": msg['id']
                })

    return archivos