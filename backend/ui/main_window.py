"""Ventana principal Tkinter para RailOps Desktop."""

from __future__ import annotations

import threading
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk

from backend.core.motor_conflictos import detectar_conflictos, limpiar_conflictos
from backend.db.sqlite_service import borrar_todo, get_connection
from backend.services.ingest_service import ejecutar_ingestion_gmail
from backend.ui.tablas_view import TablaView


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RailOps Desktop")
        self.geometry("1280x760")
        self.minsize(980, 600)

        self._build_layout()
        self._register_views()
        self.show_view("tba")

    def _build_layout(self) -> None:
        container = ttk.Frame(self)
        container.pack(fill="both", expand=True)

        self.sidebar = ttk.Frame(container, padding=10)
        self.sidebar.pack(side="left", fill="y")

        content = ttk.Frame(container, padding=(4, 8, 8, 8))
        content.pack(side="left", fill="both", expand=True)

        self.status_var = tk.StringVar(value="Listo")
        status = ttk.Label(content, textvariable=self.status_var, anchor="w")
        status.pack(fill="x", pady=(0, 6))

        self.view_container = ttk.Frame(content)
        self.view_container.pack(fill="both", expand=True)

        logs_frame = ttk.LabelFrame(content, text="Logs")
        logs_frame.pack(fill="x", pady=(8, 0))

        self.logs_text = tk.Text(logs_frame, height=7, state="disabled", wrap="word")
        self.logs_text.pack(fill="x", padx=8, pady=8)

        self._build_sidebar_buttons()

    def _build_sidebar_buttons(self) -> None:
        ttk.Label(self.sidebar, text="RailOps", font=("Segoe UI", 14, "bold")).pack(
            anchor="w", pady=(0, 10)
        )

        buttons = [
            ("Procesar Gmail", self.run_gmail_processing),
            ("🚆 Calcular Conflictos", self.calcular_conflictos),
            ("Ver TBA", lambda: self.show_view("tba")),
            ("Ver TBP", lambda: self.show_view("tbp")),
            ("Ver Mallas", lambda: self.show_view("mallas")),
            ("Ver Velocidades", lambda: self.show_view("velocidades")),
            ("Ver Conflictos", lambda: self.show_view("conflictos")),
            ("🗑 Borrar datos", self.borrar_datos),
        ]

        for text, command in buttons:
            ttk.Button(self.sidebar, text=text, command=command).pack(fill="x", pady=4)

    def clear_all_data(self) -> None:
        """Alias de compatibilidad con nombre previo."""
        self.borrar_datos()

    def borrar_datos(self) -> None:
        confirm = messagebox.askyesno(
            "Confirmar",
            "Se borrarán TODOS los datos y el control de correos. ¿Continuar?",
        )
        if not confirm:
            return

        try:
            borrar_todo()
            for key in self.views:
                self.views[key].load_data()
            self.status_var.set("Base de datos y control reiniciados")
            self.log("Base de datos y control de procesados reiniciados desde UI")
            messagebox.showinfo("OK", "Base de datos y control reiniciados")
        except Exception as exc:  # pragma: no cover - protección UI
            self.log(f"Error al borrar datos: {exc}")
            messagebox.showerror("RailOps", f"No se pudieron borrar los datos:\n{exc}")

    def calcular_conflictos(self) -> None:
        conn = get_connection()
        try:
            # limpiar conflictos anteriores
            limpiar_conflictos(conn)

            # ejecutar cálculo
            conflictos = detectar_conflictos(conn)

            self.cargar_conflictos()
            self.status_var.set(f"Conflictos recalculados: {len(conflictos)}")
            self.log(f"Conflictos recalculados correctamente: {len(conflictos)}")
            messagebox.showinfo("Conflictos", f"Conflictos detectados: {len(conflictos)}")
        except Exception as exc:  # pragma: no cover - protección UI
            self.status_var.set("Error al calcular conflictos")
            self.log(f"Error en cálculo de conflictos: {exc}")
            messagebox.showerror("Conflictos", f"Error al calcular conflictos:\n{exc}")
        finally:
            conn.close()

    def cargar_conflictos(self) -> None:
        total = self.views["conflictos"].load_data()
        self.views["conflictos"].tkraise()
        self.log(f"Tabla de conflictos recargada ({total} registros)")

    def _register_views(self) -> None:
        self.views = {
            "tba": TablaView(self.view_container, "tba"),
            "tbp": TablaView(self.view_container, "tbp"),
            "mallas": TablaView(self.view_container, "mallas"),
            "velocidades": TablaView(self.view_container, "velocidades"),
            "conflictos": TablaView(self.view_container, "conflictos"),
        }

        for view in self.views.values():
            view.place(relx=0, rely=0, relwidth=1, relheight=1)

    def show_view(self, key: str) -> None:
        view = self.views[key]
        total = view.load_data()
        view.tkraise()

        title = view.title_label.cget("text")
        self.status_var.set(f"{title} cargada: {total} registros")
        self.log(f"Vista cargada -> {title} ({total} registros)")

    def run_gmail_processing(self) -> None:
        self.status_var.set("Procesando Gmail... puede tardar unos minutos")
        self.log("Inicio de procesamiento Gmail")

        worker = threading.Thread(target=self._run_gmail_worker, daemon=True)
        worker.start()

    def _run_gmail_worker(self) -> None:
        try:
            resumen = ejecutar_ingestion_gmail()
            self.after(0, lambda: self._on_gmail_finished(resumen))
        except Exception as exc:  # pragma: no cover - protección en UI
            self.after(0, lambda: self._on_gmail_error(exc))

    def _on_gmail_finished(self, resumen: dict) -> None:
        docs = resumen.get("total_documentos_procesados", 0)
        rows = resumen.get("total_registros_insertados", 0)
        ignored = len(resumen.get("ignorados", []))

        self.status_var.set(f"Gmail procesado: documentos={docs}, filas={rows}, ignorados={ignored}")
        self.log(f"Procesamiento Gmail completado: {resumen}")

        for key in self.views:
            self.views[key].load_data()

        messagebox.showinfo("RailOps", "Procesamiento Gmail finalizado.")

    def _on_gmail_error(self, exc: Exception) -> None:
        self.status_var.set("Error al procesar Gmail")
        self.log(f"Error en procesamiento Gmail: {exc}")
        messagebox.showerror("RailOps", f"Error al procesar Gmail:\n{exc}")

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}\n"

        self.logs_text.configure(state="normal")
        self.logs_text.insert("end", line)
        self.logs_text.see("end")
        self.logs_text.configure(state="disabled")


if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()
