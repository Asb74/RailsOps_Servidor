"""Generador de borradores de correos operativos para conflictos RailOps.

Este módulo NO envía correos. Solo construye texto de borrador
según el tipo de destinatario.
"""

from __future__ import annotations

from typing import Any


def _valor(conflicto: dict[str, Any], clave: str, etiqueta: str) -> str | None:
    """Devuelve una línea formateada solo si el dato existe y no está vacío."""
    valor = conflicto.get(clave)
    if valor is None:
        return None

    if isinstance(valor, str):
        valor = valor.strip()
        if not valor:
            return None

    return f"- {etiqueta}: {valor}"


def _bloque_detalle_conflicto(conflicto: dict[str, Any]) -> str:
    """Construye un bloque con solo los campos presentes en la tabla conflictos."""
    campos = (
        ("id", "ID conflicto"),
        ("tren", "Tren"),
        ("linea", "Línea"),
        ("pk", "PK"),
        ("hora", "Hora"),
        ("tipo_conflicto", "Tipo"),
        ("descripcion", "Descripción"),
        ("accion", "Acción sugerida"),
        ("documento_origen", "Documento origen"),
        ("fecha_detectado", "Fecha detección"),
    )

    lineas = ["Detalle del conflicto:"]
    for clave, etiqueta in campos:
        linea = _valor(conflicto, clave, etiqueta)
        if linea:
            lineas.append(linea)

    if len(lineas) == 1:
        lineas.append("- Sin datos disponibles.")

    return "\n".join(lineas)


def generar_correo_maquinista(conflicto: dict[str, Any]) -> str:
    """Genera borrador formal y operativo dirigido al maquinista."""
    detalle = _bloque_detalle_conflicto(conflicto)
    return (
        "Asunto: Aviso operativo por conflicto detectado\n\n"
        "Estimado/a Maquinista:\n\n"
        "Se informa la detección de un conflicto operativo que puede afectar su circulación. "
        "Revise los antecedentes y ajuste su conducción conforme a la instrucción vigente.\n\n"
        f"{detalle}\n\n"
        "Acción inmediata:\n"
        "- Confirmar recepción del aviso por el canal operativo habitual.\n"
        "- Mantener cumplimiento estricto de señales e instrucciones de mando.\n\n"
        "Este mensaje corresponde a un borrador interno de RailOps (no enviado automáticamente)."
    )


def generar_correo_jefe(conflicto: dict[str, Any]) -> str:
    """Genera borrador formal y operativo dirigido a jefatura."""
    detalle = _bloque_detalle_conflicto(conflicto)
    return (
        "Asunto: Notificación de conflicto operativo para coordinación\n\n"
        "Estimado/a Jefe/a de Operaciones:\n\n"
        "Se registra un conflicto operativo detectado por RailOps. "
        "Se solicita coordinación de medidas y confirmación de directriz al personal involucrado.\n\n"
        f"{detalle}\n\n"
        "Gestión sugerida:\n"
        "- Validar impacto en la programación del servicio.\n"
        "- Definir instrucción operativa y canalizarla a maquinistas y oficina.\n\n"
        "Este mensaje corresponde a un borrador interno de RailOps (no enviado automáticamente)."
    )


def generar_correo_oficina(conflicto: dict[str, Any]) -> str:
    """Genera borrador formal y operativo dirigido a oficina de control/planificación."""
    detalle = _bloque_detalle_conflicto(conflicto)
    return (
        "Asunto: Registro y seguimiento de conflicto operativo\n\n"
        "Estimado equipo de Oficina:\n\n"
        "Se notifica conflicto operativo detectado para su registro y seguimiento administrativo. "
        "Favor actualizar trazabilidad y apoyar la coordinación de ajustes de servicio.\n\n"
        f"{detalle}\n\n"
        "Tareas sugeridas:\n"
        "- Registrar incidencia en el sistema de control documental.\n"
        "- Dar seguimiento a la instrucción definida por jefatura.\n\n"
        "Este mensaje corresponde a un borrador interno de RailOps (no enviado automáticamente)."
    )


__all__ = [
    "generar_correo_maquinista",
    "generar_correo_jefe",
    "generar_correo_oficina",
]
