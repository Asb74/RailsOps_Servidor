"""Compatibilidad retroactiva para el motor de conflictos.

Este módulo reexporta el servicio actual ubicado en `conflict_service.py`.
"""

from backend.core.conflict_service import (
    calcular_conflictos,
    construir_tramos_malla,
    detectar_conflictos,
    detectar_conflictos_tba,
    detectar_conflictos_tbp,
    detectar_conflictos_velocidad,
    hay_solape_pk,
    hay_solape_temporal,
    insertar_conflictos,
    limpiar_conflictos,
    normalizar_intervalo_datetime,
    safe_float,
)

__all__ = [
    "safe_float",
    "hay_solape_pk",
    "normalizar_intervalo_datetime",
    "hay_solape_temporal",
    "construir_tramos_malla",
    "limpiar_conflictos",
    "insertar_conflictos",
    "detectar_conflictos_tba",
    "detectar_conflictos_tbp",
    "detectar_conflictos_velocidad",
    "calcular_conflictos",
    "detectar_conflictos",
]
