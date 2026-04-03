"""Servicio de envío de correo.
Responsabilidad: encapsular envío de notificaciones por email para operaciones.
"""

import smtplib
from email.message import EmailMessage


def enviar_mail(smtp_host, smtp_port, usuario, password, destinatario, asunto, cuerpo):
    """Esqueleto funcional mínimo para envío SMTP con TLS."""
    msg = EmailMessage()
    msg["From"] = usuario
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.set_content(cuerpo)

    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(usuario, password)
        smtp.send_message(msg)

    return True
