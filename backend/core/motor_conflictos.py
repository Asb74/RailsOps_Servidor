"""Motor de conflictos ferroviarios.
Responsabilidad: detectar solapes y conflictos operativos entre restricciones.
"""


def detectar_conflictos(restricciones):
    """Esqueleto funcional mínimo.

    Mantiene la interfaz para evolucionar reglas de conflicto sin acoplar la UI.
    """

    conflictos = []

    for idx, item in enumerate(restricciones):
        for other in restricciones[idx + 1 :]:
            if item.get("linea") and item.get("linea") == other.get("linea"):
                if item.get("fecha_inicio") == other.get("fecha_inicio"):
                    conflictos.append({"a": item, "b": other, "motivo": "solape_misma_linea"})

    return conflictos
