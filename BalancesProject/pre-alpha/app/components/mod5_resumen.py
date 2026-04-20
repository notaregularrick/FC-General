from dash import dcc, html, Input, Output, State, no_update
import dash_bootstrap_components as dbc

from app.querys import (
    query_meses_balance,
    query_ingresos_mes,
    query_egresos_mes,
    query_saldo_inicial_total_mes,
    query_saldo_final_total_mes,
)
from app.utils import _fmt_money

def get_layout():
    return dbc.Container([
        # Título
        html.Div([
            html.H2("Resumen Mensual", className="text-center mb-4 text-primary"),
            html.P("Resumen de ingresos, egresos y saldos del mes seleccionado",
                   className="text-center text-muted mb-4")
        ], className="mb-4"),

        # Filtros
        dbc.Card([
            dbc.CardHeader("Filtros", className="bg-primary text-white py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Mes", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-mes-resumen",
                            options=[],
                            placeholder="Seleccione un mes (YYYY-MM)",
                            multi=False,
                            className="mt-1",
                            clearable=True,
                        ),
                    ], width=12, lg=6, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("🔍 Consultar Resumen", id="btn-consultar-resumen", color="primary",
                                   className="w-100 py-2", size="lg"),
                    ], width=12, className="mt-3 text-center"),
                ]),
            ]),
        ], className="mb-4 shadow-sm"),

        # Resultado del resumen
        html.Div(id="resultado-resumen", className="mb-4"),
        
        # Alerta de diferencial cambiario (si aplica)
        html.Div(id="alerta-diferencial-cambiario-resumen", className="mb-4"),

        # Detalles del resumen
        dbc.Card([
            dbc.CardHeader("Detalles del Resumen Mensual", className="bg-light py-2"),
            dbc.CardBody([
                # Primera fila: Ingresos y Egresos
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Total Ingresos (Créditos)", className="text-center py-2 bg-success text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-ingresos", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=6, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Total Egresos (Débitos)", className="text-center py-2 bg-danger text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-egresos", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=6, className="mb-3"),
                ]),
                # Segunda fila: Ganancia/Pérdida
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Flujo de caja", className="text-center py-2 bg-info text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-ganancia-perdida", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, className="mb-3"),
                ]),
                # Tercera fila: Saldos
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Inicial del Mes", className="text-center py-2 bg-secondary text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-inicial-mes", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Final Teórico", className="text-center py-2 bg-warning text-dark"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-final-teorico", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Final Real", className="text-center py-2 bg-primary text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-final-real", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                ]),
                # Cuarta fila: Diferencia (Diferencial Cambiario)
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Diferencial Cambiario", className="text-center py-2 bg-dark text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-diferencia-cambiario", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, className="mb-3"),
                ]),
            ]),
        ], id="card-detalles-resumen", className="mb-4 shadow-sm", style={"display": "none"}),

        # Interval para cargar datos automáticamente
        dcc.Interval(id='interval-carga-inicial-resumen', interval=1000, n_intervals=0, max_intervals=1)
    ], fluid=True, className="py-4")


def register_callbacks(app):
    # Inicializar opciones de filtros
    @app.callback(
        Output("input-mes-resumen", "options"),
        [Input('interval-carga-inicial-resumen', 'n_intervals')],
        prevent_initial_call=False
    )
    def inicializar_dropdown_mes(n_intervals):
        if n_intervals is None or n_intervals == 0:
            return []
        try:
            meses = query_meses_balance()
            opciones_mes = [{"label": mes, "value": mes} for mes in meses]
            return opciones_mes
        except Exception:
            return []

    # Consultar resumen
    @app.callback(
        [
            Output("resultado-resumen", "children"),
            Output("card-detalles-resumen", "style"),
            Output("detalle-ingresos", "children"),
            Output("detalle-egresos", "children"),
            Output("detalle-ganancia-perdida", "children"),
            Output("detalle-saldo-inicial-mes", "children"),
            Output("detalle-saldo-final-teorico", "children"),
            Output("detalle-saldo-final-real", "children"),
            Output("detalle-diferencia-cambiario", "children"),
            Output("alerta-diferencial-cambiario-resumen", "children"),
        ],
        [Input("btn-consultar-resumen", "n_clicks")],
        [State("input-mes-resumen", "value")],
        prevent_initial_call=True
    )
    def consultar_resumen(n_clicks, mes_seleccionado):
        if not n_clicks:
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update

        if not mes_seleccionado:
            return (
                dbc.Alert("Por favor seleccione un mes para consultar el resumen.", color="warning"),
                {"display": "none"},
                "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00",
                None
            )

        try:
            # Obtener datos
            ingresos = float(query_ingresos_mes(mes_seleccionado) or 0.0)
            egresos = float(query_egresos_mes(mes_seleccionado) or 0.0)
            saldo_inicial = float(query_saldo_inicial_total_mes(mes_seleccionado) or 0.0)
            saldo_final_real = float(query_saldo_final_total_mes(mes_seleccionado) or 0.0)


            # Calcular ganancia/pérdida bruta
            ganancia_perdida = ingresos + egresos

            # Calcular saldo final teórico
            saldo_final_teorico = saldo_inicial + ganancia_perdida

            # Calcular diferencia (diferencial cambiario)
            diferencia = abs(saldo_final_real - saldo_final_teorico)

            # Formatear valores
            ingresos_fmt = _fmt_money(ingresos)
            egresos_fmt = _fmt_money(egresos)
            ganancia_perdida_fmt = _fmt_money(ganancia_perdida)
            saldo_inicial_fmt = _fmt_money(saldo_inicial)
            saldo_final_teorico_fmt = _fmt_money(saldo_final_teorico)
            saldo_final_real_fmt = _fmt_money(saldo_final_real)
            diferencia_fmt = _fmt_money(diferencia)

            # Determinar si hay diferencial cambiario significativo
            # Tolerancia: 50 unidades
            tolerancia = 50.0
            hay_diferencial = diferencia > tolerancia

            # Crear alerta de resultado
            if ganancia_perdida >= 0:
                tipo_resultado = "success"
                texto_resultado = f"Ganancia Bruta: {ganancia_perdida_fmt}"
            else:
                tipo_resultado = "danger"
                texto_resultado = f"Pérdida Bruta: {ganancia_perdida_fmt}"

            alerta = dbc.Alert(
                [
                    html.H4("✓ Resumen Mensual Calculado", className="alert-heading"),
                    html.P(f"Mes: {mes_seleccionado}", className="mb-2"),
                    html.P(f"Total Ingresos: {ingresos_fmt} | Total Egresos: {egresos_fmt}", className="mb-2"),
                    html.P(texto_resultado, className="mb-0 fw-bold"),
                ],
                color=tipo_resultado,
                className="mb-0"
            )

            # Crear alerta de diferencial cambiario si aplica
            alerta_diferencial = None
            if hay_diferencial:
                porcentaje_diferencia = 0.0
                if saldo_final_real != 0:
                    porcentaje_diferencia = (diferencia / abs(saldo_final_real)) * 100
                elif saldo_final_teorico != 0:
                    porcentaje_diferencia = (diferencia / abs(saldo_final_teorico)) * 100

                alerta_diferencial = dbc.Alert(
                    [
                        html.H4("ℹ Diferencial Cambiario Detectado", className="alert-heading"),
                        html.P(f"La diferencia de {diferencia_fmt} ({porcentaje_diferencia:.2f}%) entre el saldo final real y teórico probablemente se debe a diferencial cambiario."),
                        html.P("Los movimientos se registraron con tasas de cambio diferentes a las utilizadas para calcular los saldos finales.", className="mb-0"),
                        html.Hr(),
                        html.P(f"Mes: {mes_seleccionado}", className="mb-0")
                    ],
                    color="info",
                    className="mb-0"
                )

            return (
                alerta,
                {"display": "block"},
                ingresos_fmt,
                egresos_fmt,
                ganancia_perdida_fmt,
                saldo_inicial_fmt,
                saldo_final_teorico_fmt,
                saldo_final_real_fmt,
                diferencia_fmt,
                alerta_diferencial
            )

        except Exception as e:
            return (
                dbc.Alert(f"Error al consultar el resumen: {str(e)}", color="danger"),
                {"display": "none"},
                "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00",
                None
            )