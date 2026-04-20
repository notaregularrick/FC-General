from dash import dcc, html, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc
import pandas as pd

from app.querys import (
    query_bancos,
    query_meses_balance,
    query_saldo_inicial_banco_mes,
    query_saldo_final_banco_mes,
    query_suma_movimientos_banco_mes,
    query_moneda_ref_banco_mes,
)
from app.utils import _fmt_money

def get_layout():
    return dbc.Container([
        # Título
        html.Div([
            html.H2("Cuadre de Cuentas", className="text-center mb-4 text-primary"),
            html.P("Verifique que los movimientos del mes cuadren con los saldos inicial y final",
                   className="text-center text-muted mb-4")
        ], className="mb-4"),

        # Filtros
        dbc.Card([
            dbc.CardHeader("Filtros", className="bg-primary text-white py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Banco", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-banco-cuadre",
                            options=[],
                            placeholder="Seleccione un banco",
                            multi=False,
                            className="mt-1"
                        ),
                    ], width=12, lg=6, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Mes", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-mes-cuadre",
                            options=[],
                            placeholder="Seleccione un mes",
                            multi=False,
                            className="mt-1",
                            clearable=True,
                        ),
                    ], width=12, lg=6, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("🔍 Verificar Cuadre", id="btn-verificar-cuadre", color="primary",
                                   className="w-100 py-2", size="lg"),
                    ], width=12, className="mt-3 text-center"),
                ]),
            ]),
        ], className="mb-4 shadow-sm"),

        # Resultado del cuadre
        html.Div(id="resultado-cuadre", className="mb-4"),
        
        # Alerta de diferencial cambiario (si aplica)
        html.Div(id="alerta-diferencial-cambiario", className="mb-4"),

        # Detalles del cálculo
        dbc.Card([
            dbc.CardHeader("Detalles del Cálculo", className="bg-light py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Inicial", className="text-center py-2 bg-info text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-inicial", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Movimientos del Mes", className="text-center py-2 bg-warning text-dark"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-movimientos", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Calculado", className="text-center py-2 bg-secondary text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-calculado", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=4, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Saldo Final Esperado", className="text-center py-2 bg-success text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-saldo-final", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=6, className="mb-3"),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Diferencia", className="text-center py-2 bg-danger text-white"),
                            dbc.CardBody([
                                html.H4("$ 0,00", id="detalle-diferencia", className="text-center mb-0")
                            ])
                        ], className="h-100")
                    ], width=12, md=6, className="mb-3"),
                ]),
            ]),
        ], id="card-detalles", className="mb-4 shadow-sm", style={"display": "none"}),

        # Interval para cargar datos automáticamente
        dcc.Interval(id='interval-carga-inicial-cuadre', interval=1000, n_intervals=0, max_intervals=1)
    ], fluid=True, className="py-4")


def register_callbacks(app):
    # Inicializar opciones de filtros
    @app.callback(
        [
            Output("input-banco-cuadre", "options"),
            Output("input-mes-cuadre", "options"),
        ],
        [Input('interval-carga-inicial-cuadre', 'n_intervals')],
        prevent_initial_call=False
    )
    def inicializar_dropdowns(n_intervals):
        if n_intervals is None or n_intervals == 0:
            return [], []
        try:
            bancos = query_bancos()
            meses = query_meses_balance()
            opciones_banco = [{"label": banco[0], "value": banco[0]} for banco in bancos]
            opciones_mes = [{"label": mes, "value": mes} for mes in meses]
            return opciones_banco, opciones_mes
        except Exception:
            return [], []

    # Verificar cuadre
    @app.callback(
        [
            Output("resultado-cuadre", "children"),
            Output("card-detalles", "style"),
            Output("detalle-saldo-inicial", "children"),
            Output("detalle-movimientos", "children"),
            Output("detalle-saldo-calculado", "children"),
            Output("detalle-saldo-final", "children"),
            Output("detalle-diferencia", "children"),
            Output("alerta-diferencial-cambiario", "children"),
        ],
        [Input("btn-verificar-cuadre", "n_clicks")],
        [
            State("input-banco-cuadre", "value"),
            State("input-mes-cuadre", "value")
        ],
        prevent_initial_call=True
    )
    def verificar_cuadre(n_clicks, banco_seleccionado, mes_seleccionado):
        if not n_clicks:
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update

        if not banco_seleccionado or not mes_seleccionado:
            return (
                dbc.Alert("Por favor seleccione un banco y un mes para verificar el cuadre.", color="warning"),
                {"display": "none"},
                "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00",
                None
            )

        try:
            # Obtener datos y convertir a float para evitar problemas de tipos
            saldo_inicial = float(query_saldo_inicial_banco_mes(banco_seleccionado, mes_seleccionado) or 0.0)
            saldo_final = float(query_saldo_final_banco_mes(banco_seleccionado, mes_seleccionado) or 0.0)
            suma_movimientos = float(query_suma_movimientos_banco_mes(banco_seleccionado, mes_seleccionado) or 0.0)
            moneda_ref = query_moneda_ref_banco_mes(banco_seleccionado, mes_seleccionado)

            # Calcular saldo esperado
            saldo_calculado = saldo_inicial + suma_movimientos
            diferencia = abs(saldo_final - saldo_calculado)

            # Tolerancia: 1-2 unidades arriba o abajo
            tolerancia = 50.0

            # Calcular porcentaje de diferencia (respecto al saldo final)
            porcentaje_diferencia = 0.0
            if saldo_final != 0:
                porcentaje_diferencia = (diferencia / abs(saldo_final)) * 100
            elif saldo_calculado != 0:
                porcentaje_diferencia = (diferencia / abs(saldo_calculado)) * 100

            # Formatear valores para mostrar
            saldo_inicial_fmt = _fmt_money(saldo_inicial)
            movimientos_fmt = _fmt_money(suma_movimientos)
            saldo_calculado_fmt = _fmt_money(saldo_calculado)
            saldo_final_fmt = _fmt_money(saldo_final)
            diferencia_fmt = _fmt_money(diferencia)

            # Verificar si es BS y la diferencia es menor al 50% (diferencial cambiario)
            hay_diferencial_cambiario = (moneda_ref == "BS" and porcentaje_diferencia < 100.0 and diferencia > tolerancia)


            
            # Verificar si cuadra
            if diferencia <= tolerancia:
                alerta = dbc.Alert(
                    [
                        html.H4("✓ Cuenta Cuadrada", className="alert-heading"),
                        html.P(f"El saldo calculado ({saldo_calculado_fmt}) coincide con el saldo final ({saldo_final_fmt}) "
                               f"dentro de la tolerancia permitida (±{tolerancia})."),
                        html.Hr(),
                        html.P(f"Banco: {banco_seleccionado} | Mes: {mes_seleccionado}", className="mb-0")
                    ],
                    color="success",
                    className="mb-0"
                )
                alerta_diferencial = None
            elif hay_diferencial_cambiario:
                # Si hay diferencial cambiario, no mostrar alerta de "Faltan Movimientos"
                alerta = None
                alerta_diferencial = dbc.Alert(
                    [
                        html.H4("ℹ Diferencial Cambiario", className="alert-heading"),
                        html.P(f"La diferencia de {diferencia_fmt} ({porcentaje_diferencia:.2f}%) probablemente se debe a diferencial cambiario."),
                        html.P("Los movimientos se registraron con tasas de cambio diferentes a las utilizadas para calcular los saldos finales.", className="mb-0"),
                        html.Hr(),
                        html.P(f"Banco: {banco_seleccionado} | Mes: {mes_seleccionado}", className="mb-0")
                    ],
                    color="info",
                    className="mb-0"
                )
            else:
                # Solo mostrar alerta de "Faltan Movimientos" si NO hay diferencial cambiario
                alerta = dbc.Alert(
                    [
                        html.H4("⚠ Faltan Movimientos", className="alert-heading"),
                        html.P(f"El saldo calculado ({saldo_calculado_fmt}) no coincide con el saldo final ({saldo_final_fmt})."),
                        html.P(f"Diferencia: {diferencia_fmt}", className="mb-0 fw-bold"),
                        html.Hr(),
                        html.P(f"Banco: {banco_seleccionado} | Mes: {mes_seleccionado}", className="mb-0")
                    ],
                    color="warning",
                    className="mb-0"
                )
                alerta_diferencial = None

            return (
                alerta,
                {"display": "block"},
                saldo_inicial_fmt,
                movimientos_fmt,
                saldo_calculado_fmt,
                saldo_final_fmt,
                diferencia_fmt,
                alerta_diferencial
            )

        except Exception as e:
            return (
                dbc.Alert(f"Error al verificar el cuadre: {str(e)}", color="danger"),
                {"display": "none"},
                "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00", "$ 0,00",
                None
            )
