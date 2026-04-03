import firebase_admin
from firebase_admin import credentials, firestore
from config import FIREBASE_CREDENTIALS


def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def guardar_documento(nombre, tipo, gmail_id):
    db = init_firebase()

    doc_ref = db.collection("documentos").add({
        "nombre": nombre,
        "tipo": tipo,
        "gmail_id": gmail_id,
        "procesado": True
    })

    return doc_ref[1].id


def guardar_restricciones(data):
    db = init_firebase()

    for item in data:
        db.collection("restricciones").add(item)