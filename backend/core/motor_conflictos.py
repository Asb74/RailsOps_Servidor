"""Compatibilidad retroactiva para el motor de conflictos.

Este módulo reexporta el servicio actual ubicado en `conflict_service.py`.
"""

from backend.core.conflict_service import (
    calcular_conflictos,
    detectar_conflictos,
    detectar_conflictos_tba,
    detectar_conflictos_tbp,
    detectar_conflictos_velocidad,
    insertar_conflictos,
    limpiar_conflictos,
    safe_float,
)

__all__ = [
    "safe_float",
    "limpiar_conflictos",
    "insertar_conflictos",
    "detectar_conflictos_tba",
    "detectar_conflictos_tbp",
    "detectar_conflictos_velocidad",
    "calcular_conflictos",
    "detectar_conflictos",
]

