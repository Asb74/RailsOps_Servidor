"""Servicio coordinador de ingestión automática Gmail -> SQLite."""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from backend.core.parser_malla import procesar_malla
from backend.core.parser_tba import procesar_tba
from backend.core.parser_tbp import procesar_tbp
from backend.core.parser_velocidades import procesar_velocidades
from backend.db import sqlite_service
from backend.services.gmail_reader import clasificar_documento, descargar_adjuntos, marcar_email_como_leido
from backend.services.processing_control import marcar_procesado, ya_procesado

logger = logging.getLogger(__name__)


def _procesar_por_tipo(file_path: str, tipo: str, documento_id: int) -> list[dict[str, Any]]:
    if tipo == "TBA":
        return procesar_tba(file_path, documento_id, sqlite_service)
    if tipo == "TBP":
        return procesar_tbp(file_path, documento_id, sqlite_service)
    if tipo == "MALLA":
        return procesar_malla(file_path, documento_id, sqlite_service)
    if tipo == "VELOCIDADES":
        return procesar_velocidades(file_path, documento_id, sqlite_service)

    return []


def ejecutar_ingestion_gmail() -> dict[str, Any]:
    """Orquesta el flujo de ingestión automática sin Firebase.

    Flujo:
      1) Descargar adjuntos Gmail.
      2) Ignorar correos ya procesados localmente.
      3) Clasificar adjuntos útiles.
      4) Insertar documento en SQLite.
      5) Ejecutar parser específico por tipo.
      6) Marcar gmail_id como procesado localmente.
    """
    logging.basicConfig(level=logging.INFO)

    sqlite_service.init_db()
    adjuntos = descargar_adjuntos()

    agrupados: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in adjuntos:
        agrupados[item["gmail_id"]].append(item)

    total_docs = 0
    total_registros = 0
    procesados: list[dict[str, Any]] = []
    ignorados: list[dict[str, Any]] = []

    for gmail_id, items in agrupados.items():
        if ya_procesado(gmail_id):
            print(f"[GMAIL] Email ya procesado: {gmail_id}")
            logger.info("[INGEST] Correo omitido (duplicado local): gmail_id=%s", gmail_id)
            ignorados.extend(
                {"gmail_id": gmail_id, "archivo": it["filename"], "motivo": "correo_ya_procesado"}
                for it in items
            )
            continue

        print(f"[GMAIL] Email nuevo detectado: {gmail_id}")
        logger.info("[INGEST] Procesando correo gmail_id=%s adjuntos=%s", gmail_id, len(items))
        procesados_del_correo = 0

        try:
            for item in items:
                archivo = item["filename"]
                ruta = item["path"]
                tipo = clasificar_documento(item.get("original_filename") or archivo)

                if tipo is None:
                    logger.info("[INGEST] Adjunto no útil, se ignora: gmail_id=%s archivo=%s", gmail_id, archivo)
                    ignorados.append({"gmail_id": gmail_id, "archivo": archivo, "motivo": "tipo_no_soportado"})
                    continue

                if not Path(ruta).exists():
                    logger.warning("[INGEST] Archivo no encontrado tras descarga: %s", ruta)
                    ignorados.append({"gmail_id": gmail_id, "archivo": archivo, "motivo": "archivo_no_encontrado"})
                    continue

                documento_id = sqlite_service.insertar_documento(
                    nombre=archivo,
                    tipo=tipo,
                    gmail_id=gmail_id,
                )
                logger.info(
                    "[INGEST] Documento insertado en SQLite: doc_id=%s tipo=%s archivo=%s",
                    documento_id,
                    tipo,
                    archivo,
                )

                filas = _procesar_por_tipo(ruta, tipo, documento_id)
                total_docs += 1
                total_registros += len(filas)
                procesados_del_correo += 1

                procesados.append(
                    {
                        "gmail_id": gmail_id,
                        "archivo": archivo,
                        "tipo": tipo,
                        "documento_id": documento_id,
                        "registros_insertados": len(filas),
                    }
                )
                logger.info(
                    "[INGEST] Parseo completado: doc_id=%s tipo=%s filas=%s",
                    documento_id,
                    tipo,
                    len(filas),
                )

            # Marcamos el correo como procesado para no redescargar/reprocesar sus adjuntos en futuras corridas.
            # Se marca incluso si no hubo adjuntos útiles, para evitar ciclos de reintento infinitos sobre el mismo correo.
            marcar_procesado(gmail_id)
            logger.info(
                "[INGEST] Correo finalizado: gmail_id=%s adjuntos_procesados=%s adjuntos_totales=%s",
                gmail_id,
                procesados_del_correo,
                len(items),
            )
        except Exception:
            print(f"[GMAIL] Error procesando email: {gmail_id}")
            logger.exception("[INGEST] Error procesando correo gmail_id=%s", gmail_id)
        finally:
            try:
                marcar_email_como_leido(gmail_id)
                print(f"[GMAIL] Email marcado como leído: {gmail_id}")
            except Exception:
                print(f"[GMAIL] Error marcando como leído: {gmail_id}")

    resumen = {
        "total_correos_descargados": len(agrupados),
        "total_documentos_procesados": total_docs,
        "total_registros_insertados": total_registros,
        "procesados": procesados,
        "ignorados": ignorados,
    }
    logger.info("[INGEST] Resumen: %s", resumen)
    return resumen
