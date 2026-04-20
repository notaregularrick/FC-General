from dash import dcc, html, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px

from ..utils import crear_tabla_estilizada, _fmt_money
from app.querys import (
    query_meses_balance,
    query_cobranzas_total_mes,
    query_cobranzas_por_banco,
    query_cobranzas_por_moneda,
    query_cobranzas_por_cliente,
    query_cobranzas_por_semana,
    query_pagos_operativos_total_mes,
    query_pagos_operativos_por_semana,
    query_pagos_operativos_por_banco,
    query_pagos_operativos_por_cliente,
    query_pagos_operativos_por_moneda,
    query_flujo_caja_agrupado,
    query_ingresos_mes,
    query_egresos_mes,
    query_saldo_inicial_total_mes,
    query_saldo_final_total_mes,
    query_masa_monetaria_mes,
)


def get_layout():
    return dbc.Container(
        [
            # Título
            html.Div(
                [
                    html.H2(
                        "Cashflow",
                        className="text-center mb-4 text-primary",
                    ),
                    html.P(
                        "Seleccione un mes para ver el flujo de caja.",
                        className="text-center text-muted mb-4",
                    ),
                ],
                className="mb-4",
            ),

            # Filtros
            dbc.Card(
                [
                    dbc.CardHeader(
                        "Filtros", className="bg-primary text-white py-2"
                    ),
                    dbc.CardBody(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Label("Mes", className="fw-bold"),
                                            dcc.Dropdown(
                                                id="input-mes-cashflow",
                                                options=[],
                                                placeholder="Seleccione un mes (YYYY-MM)",
                                                multi=False,
                                                className="mt-1",
                                                clearable=True,
                                            ),
                                        ],
                                        width=12,
                                        lg=6,
                                        className="mb-3",
                                    ),
                                ]
                            ),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Button(
                                                "🔍 Consultar Cashflow",
                                                id="btn-consultar-cashflow",
                                                color="primary",
                                                className="w-100 py-2",
                                                size="lg",
                                            ),
                                        ],
                                        width=12,
                                        className="mt-3 text-center",
                                    )
                                ]
                            ),
                        ]
                    ),
                ],
                className="mb-4 shadow-sm",
            ),

            # Flujo de caja
            dbc.Row(
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Flujo de Caja",
                                className="bg-light py-2",
                            ),
                            dbc.CardBody(
                                [
                                    html.Div(
                                        id="tabla-flujo-caja-cashflow-container",
                                    )
                                ]
                            ),
                        ],
                        className="mb-3 shadow-sm",
                    )
                ),
            ),

            

            # Cobranzas y Pagos Operativos
            dbc.Row(
                [
                    # Tarjeta Cobranzas
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Cobranzas",
                                    className="bg-success text-white text-center py-2",
                                ),
                                dbc.CardBody(
                                    [
                                        html.P(
                                            "Monto total de cobranzas del mes seleccionado",
                                            className="text-muted mb-1",
                                        ),
                                        html.H4(
                                            _fmt_money(0),
                                            id="cobranzas-total",
                                            className="text-center mb-3",
                                        ),
                                        dbc.Label("Detalle", className="fw-bold mt-2"),
                                        dcc.Dropdown(
                                            id="cobranzas-detalle-tipo",
                                            options=[
                                                {
                                                    "label": "Cobranzas por banco",
                                                    "value": "banco",
                                                },
                                                {
                                                    "label": "Cobranzas por cliente",
                                                    "value": "cliente",
                                                },
                                                {
                                                    "label": "Cobranzas por moneda",
                                                    "value": "moneda",
                                                },
                                                {
                                                    "label": "Evolución de cobranza semanal",
                                                    "value": "semana",
                                                },
                                            ],
                                            value="semana",
                                            clearable=False,
                                            className="mt-1 mb-3",
                                        ),
                                        html.Div(
                                            id="cobranzas-detalle-container"
                                        ),
                                    ]
                                ),
                            ],
                            className="mb-3 shadow-sm h-100",
                        ),
                        width=12,
                        lg=6,
                        className="mb-3",
                    ),

                    # Tarjeta Pagos Operativos
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Pagos Operativos",
                                    className="bg-danger text-white text-center py-2",
                                ),
                                dbc.CardBody(
                                    [
                                        html.P(
                                            "Monto total de pagos operativos del mes seleccionado",
                                            className="text-muted mb-1",
                                        ),
                                        html.H4(
                                            _fmt_money(0),
                                            id="pagos-op-total",
                                            className="text-center mb-3",
                                        ),
                                        dbc.Label("Detalle", className="fw-bold mt-2"),
                                        dcc.Dropdown(
                                            id="pagos-op-detalle-tipo",
                                            options=[
                                                {
                                                    "label": "Evolución semanal de pagos operativos",
                                                    "value": "semana",
                                                },
                                                {
                                                    "label": "Pagos operativos por banco",
                                                    "value": "banco",
                                                },
                                                {
                                                    "label": "Pagos operativos por proveedor/cliente",
                                                    "value": "cliente",
                                                },
                                                {
                                                    "label": "Pagos operativos por moneda",
                                                    "value": "moneda",
                                                },
                                            ],
                                            value="semana",
                                            clearable=False,
                                            className="mt-1 mb-3",
                                        ),
                                        html.Div(
                                            id="pagos-op-detalle-container"
                                        ),
                                    ]
                                ),
                            ],
                            className="mb-3 shadow-sm h-100",
                        ),
                        width=12,
                        lg=6,
                        className="mb-3",
                    ),
                ],
                className="mb-4",
            ),

            # Detalles del Resumen Mensual (sin alertas)
            dbc.Card(
                [
                    dbc.CardHeader(
                        "Detalles del Cashflow", className="bg-light py-2"
                    ),
                    dbc.CardBody(
                        [
                            # Primera fila: Ingresos y Egresos
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Total Ingresos (Créditos)",
                                                        className="text-center py-2 bg-success text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-ingresos",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        md=6,
                                        className="mb-3",
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Total Egresos (Débitos)",
                                                        className="text-center py-2 bg-danger text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-egresos",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        md=6,
                                        className="mb-3",
                                    ),
                                ]
                            ),
                            # Segunda fila: Flujo de caja (ganancia/pérdida)
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Flujo de caja",
                                                        className="text-center py-2 bg-info text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-ganancia-perdida",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        className="mb-3",
                                    )
                                ]
                            ),
                            # Tercera fila: Saldos
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Saldo Inicial del Mes",
                                                        className="text-center py-2 bg-secondary text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-saldo-inicial-mes",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        md=4,
                                        className="mb-3",
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Saldo Final Teórico",
                                                        className="text-center py-2 bg-warning text-dark",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-saldo-final-teorico",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        md=4,
                                        className="mb-3",
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Saldo Final Real",
                                                        className="text-center py-2 bg-primary text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-saldo-final-real",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        md=4,
                                        className="mb-3",
                                    ),
                                ]
                            ),
                            # Cuarta fila: Diferencia
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Diferencial Cambiario",
                                                        className="text-center py-2 bg-dark text-white",
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.H4(
                                                                "$ 0,00",
                                                                id="cf-detalle-diferencia-cambiario",
                                                                className="text-center mb-0",
                                                            )
                                                        ]
                                                    ),
                                                ],
                                                className="h-100",
                                            )
                                        ],
                                        width=12,
                                        className="mb-3",
                                    )
                                ]
                            ),
                        ]
                    ),
                ],
                id="card-detalles-resumen-cashflow",
                className="mb-4 shadow-sm",
                style={"display": "none"},
            ),

            # Disponibilidad monetaria (a final de mes)
            dbc.Row(
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                "Disponibilidad monetaria",
                                className="bg-info text-white text-center py-2",
                            ),
                            dbc.CardBody(
                                [
                                    html.P(
                                        "Saldo a final de mes (última semana del mes) en todas las monedas.",
                                        className="text-muted text-center mb-3",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                dbc.Card(
                                                    [
                                                        dbc.CardHeader(
                                                            "Saldo a Final de Semana Total",
                                                            className="text-center py-3 bg-secondary text-white",
                                                        ),
                                                        dbc.CardBody(
                                                            [
                                                                html.H2(
                                                                    _fmt_money(0),
                                                                    id="kpi-masa-monetaria-total-mes",
                                                                    className="text-center text-white mb-0",
                                                                )
                                                            ],
                                                            className="bg-secondary",
                                                        ),
                                                    ],
                                                    className="mb-3",
                                                ),
                                                width=12,
                                                lg=4,
                                                className="mb-3",
                                            ),
                                            dbc.Col(
                                                dbc.Card(
                                                    [
                                                        dbc.CardHeader(
                                                            "Saldo a Final de Semana (Bolívares)",
                                                            className="text-center py-3 bg-success text-white",
                                                        ),
                                                        dbc.CardBody(
                                                            [
                                                                html.H2(
                                                                    _fmt_money(0, "BS"),
                                                                    id="kpi-masa-monetaria-bs-mes",
                                                                    className="text-center text-white mb-0",
                                                                )
                                                            ],
                                                            className="bg-success",
                                                        ),
                                                    ],
                                                    className="mb-3 border-0 shadow-lg",
                                                ),
                                                width=12,
                                                lg=4,
                                                className="mb-3",
                                            ),
                                            dbc.Col(
                                                dbc.Card(
                                                    [
                                                        dbc.CardHeader(
                                                            "Saldo a Final de Semana (Dólares)",
                                                            className="text-center py-3 bg-primary text-white",
                                                        ),
                                                        dbc.CardBody(
                                                            [
                                                                html.H2(
                                                                    _fmt_money(0, "US"),
                                                                    id="kpi-masa-monetaria-us-mes",
                                                                    className="text-center text-white mb-0",
                                                                )
                                                            ],
                                                            className="bg-primary",
                                                        ),
                                                    ],
                                                    className="mb-3 border-0 shadow-lg",
                                                ),
                                                width=12,
                                                lg=4,
                                                className="mb-3",
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                        ],
                        className="mb-4 shadow-sm",
                    )
                ),
            ),

            # Intervalo para carga inicial de meses
            dcc.Interval(
                id="interval-carga-inicial-cashflow",
                interval=1000,
                n_intervals=0,
                max_intervals=1,
            ),

            # Store para recordar el mes consultado
            dcc.Store(id="store-mes-cashflow"),
        ],
        fluid=True,
        className="py-4",
    )


def _crear_tabla_conceptos(table_id, registros, nombre_concepto):
    """
    Construye una tabla simple Concepto / Importe a partir de registros
    [(concepto, importe), ...].
    """
    columnas = [
        {"name": nombre_concepto, "id": "Concepto"},
        {"name": "Importe", "id": "Importe"},
    ]

    data = []
    for concepto, importe in registros:
        data.append(
            {
                "Concepto": concepto,
                "Importe": _fmt_money(float(importe or 0.0)),
            }
        )

    return crear_tabla_estilizada(
        id=table_id,
        columns=columnas,
        data=data,
        page_size=2000,
        estilo_adicional=[],
    )


def register_callbacks(app):
    # Inicializar opciones de meses
    @app.callback(
        Output("input-mes-cashflow", "options"),
        [Input("interval-carga-inicial-cashflow", "n_intervals")],
        prevent_initial_call=False,
    )
    def inicializar_dropdown_mes_cashflow(n_intervals):
        if n_intervals is None or n_intervals == 0:
            return []
        try:
            meses = query_meses_balance()
            return [{"label": mes, "value": mes} for mes in meses]
        except Exception:
            return []

    # Consultar totales de cobranzas y pagos operativos
    @app.callback(
        [
            Output("store-mes-cashflow", "data"),
            Output("cobranzas-total", "children"),
            Output("pagos-op-total", "children"),
        ],
        [Input("btn-consultar-cashflow", "n_clicks")],
        [State("input-mes-cashflow", "value")],
        prevent_initial_call=True,
    )
    def consultar_totales_cashflow(n_clicks, mes_seleccionado):
        if not n_clicks:
            return no_update, no_update, no_update

        if not mes_seleccionado:
            # Sin mes: mostramos 0 y no guardamos mes
            return None, _fmt_money(0), _fmt_money(0)

        try:
            total_cobranzas = float(query_cobranzas_total_mes(mes_seleccionado) or 0.0)
            total_pagos = float(
                query_pagos_operativos_total_mes(mes_seleccionado) or 0.0
            )

            return (
                mes_seleccionado,
                _fmt_money(total_cobranzas),
                _fmt_money(total_pagos),
            )
        except Exception:
            # En error mostramos 0 pero mantenemos el mes
            return mes_seleccionado, _fmt_money(0), _fmt_money(0)

    # Detalles del resumen mensual (sin alertas)
    @app.callback(
        [
            Output("card-detalles-resumen-cashflow", "style"),
            Output("cf-detalle-ingresos", "children"),
            Output("cf-detalle-egresos", "children"),
            Output("cf-detalle-ganancia-perdida", "children"),
            Output("cf-detalle-saldo-inicial-mes", "children"),
            Output("cf-detalle-saldo-final-teorico", "children"),
            Output("cf-detalle-saldo-final-real", "children"),
            Output("cf-detalle-diferencia-cambiario", "children"),
        ],
        [Input("store-mes-cashflow", "data")],
        prevent_initial_call=False,
    )
    def actualizar_detalles_resumen_mensual(mes_consultado):
        if not mes_consultado:
            return (
                {"display": "none"},
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
            )

        try:
            ingresos = float(query_ingresos_mes(mes_consultado) or 0.0)
            egresos = float(query_egresos_mes(mes_consultado) or 0.0)
            saldo_inicial = float(query_saldo_inicial_total_mes(mes_consultado) or 0.0)
            saldo_final_real = float(query_saldo_final_total_mes(mes_consultado) or 0.0)

            flujo_caja = ingresos + egresos
            saldo_final_teorico = saldo_inicial + flujo_caja
            diferencia = abs(saldo_final_real - saldo_final_teorico)

            return (
                {"display": "block"},
                _fmt_money(ingresos),
                _fmt_money(egresos),
                _fmt_money(flujo_caja),
                _fmt_money(saldo_inicial),
                _fmt_money(saldo_final_teorico),
                _fmt_money(saldo_final_real),
                _fmt_money(diferencia),
            )
        except Exception:
            return (
                {"display": "none"},
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
                "$ 0,00",
            )

    # KPIs de disponibilidad monetaria (final de mes)
    @app.callback(
        [
            Output("kpi-masa-monetaria-total-mes", "children"),
            Output("kpi-masa-monetaria-bs-mes", "children"),
            Output("kpi-masa-monetaria-us-mes", "children"),
        ],
        [Input("store-mes-cashflow", "data")],
        prevent_initial_call=False,
    )
    def actualizar_disponibilidad_monetaria(mes_consultado):
        if not mes_consultado:
            # Sin mes seleccionado: mostramos 0 en todos los KPIs
            return _fmt_money(0), _fmt_money(0, "BS"), _fmt_money(0, "US")

        try:
            total = float(query_saldo_final_total_mes(mes_consultado) or 0.0)
            bs = float(query_masa_monetaria_mes("BS", mes_consultado) or 0.0)
            us = float(query_masa_monetaria_mes("US", mes_consultado) or 0.0)

            return (
                _fmt_money(total),
                _fmt_money(bs, "BS"),
                _fmt_money(us, "US"),
            )
        except Exception:
            return _fmt_money(0), _fmt_money(0, "BS"), _fmt_money(0, "US")

    # Flujo de caja por concepto general (tabla)
    @app.callback(
        Output("tabla-flujo-caja-cashflow-container", "children"),
        [Input("store-mes-cashflow", "data")],
        prevent_initial_call=False,
    )
    def actualizar_flujo_caja_concepto_general(mes_consultado):
        if not mes_consultado:
            return dbc.Alert(
                "Seleccione un mes y pulse 'Consultar Cashflow' para ver el flujo de caja.",
                color="warning",
                className="mb-0",
            )

        try:
            registros = query_flujo_caja_agrupado(None, mes_consultado)
            if not registros:
                return dbc.Alert(
                    "No hay datos de flujo de caja para el mes seleccionado.",
                    color="info",
                    className="mb-0",
                )

            return _crear_tabla_conceptos(
                "tabla-flujo-caja-cashflow",
                registros,
                "Concepto",
            )
        except Exception:
            return dbc.Alert(
                "Ocurrió un error al obtener el flujo de caja.",
                color="danger",
                className="mb-0",
            )

    # Detalle de cobranzas
    @app.callback(
        Output("cobranzas-detalle-container", "children"),
        [
            Input("cobranzas-detalle-tipo", "value"),
            Input("store-mes-cashflow", "data"),
        ],
        prevent_initial_call=False,
    )
    def actualizar_detalle_cobranzas(tipo, mes_consultado):
        if not mes_consultado:
            return dbc.Alert(
                "Seleccione un mes y pulse 'Consultar Cashflow' para ver el detalle de cobranzas.",
                color="warning",
                className="mb-0",
            )

        try:
            if tipo == "banco":
                registros = query_cobranzas_por_banco(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay cobranzas registradas para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                # registros: [(banco, importe), ...]
                return _crear_tabla_conceptos(
                    "tabla-cobranzas-banco", registros, "Banco"
                )

            if tipo == "cliente":
                registros = query_cobranzas_por_cliente(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay cobranzas por cliente para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                return _crear_tabla_conceptos(
                    "tabla-cobranzas-cliente", registros, "Cliente"
                )

            if tipo == "moneda":
                registros = query_cobranzas_por_moneda(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay cobranzas por moneda para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                return _crear_tabla_conceptos(
                    "tabla-cobranzas-moneda", registros, "Moneda"
                )

            if tipo == "semana":
                registros = query_cobranzas_por_semana(mes_consultado)
                df = pd.DataFrame(registros, columns=["semana_mes", "importe"])
                if df.empty:
                    return dbc.Alert(
                        "No hay datos semanales de cobranzas para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                fig = px.line(
                    df,
                    x="semana_mes",
                    y="importe",
                    markers=True,
                    labels={
                        "semana_mes": "Semana del mes",
                        "importe": "Monto (USD)",
                    },
                    title=f"Evolución semanal de cobranzas - {mes_consultado}",
                )
                fig.update_layout(template="plotly_white", height=350)
                return dcc.Graph(id="grafico-cobranzas-semanal", figure=fig)

            # Valor inesperado
            return dbc.Alert(
                "Seleccione un tipo de detalle válido para cobranzas.",
                color="warning",
                className="mb-0",
            )

        except Exception:
            return dbc.Alert(
                "Ocurrió un error al obtener el detalle de cobranzas.",
                color="danger",
                className="mb-0",
            )

    # Detalle de pagos operativos
    @app.callback(
        Output("pagos-op-detalle-container", "children"),
        [
            Input("pagos-op-detalle-tipo", "value"),
            Input("store-mes-cashflow", "data"),
        ],
        prevent_initial_call=False,
    )
    def actualizar_detalle_pagos_operativos(tipo, mes_consultado):
        if not mes_consultado:
            return dbc.Alert(
                "Seleccione un mes y pulse 'Consultar Cashflow' para ver el detalle de pagos operativos.",
                color="warning",
                className="mb-0",
            )

        try:
            if tipo == "semana":
                registros = query_pagos_operativos_por_semana(mes_consultado)
                df = pd.DataFrame(registros, columns=["semana_mes", "importe"])
                if df.empty:
                    return dbc.Alert(
                        "No hay datos semanales de pagos operativos para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )

                fig = px.line(
                    df,
                    x="semana_mes",
                    y="importe",
                    markers=True,
                    labels={
                        "semana_mes": "Semana del mes",
                        "importe": "Monto (USD)",
                    },
                    title=f"Evolución semanal de pagos operativos - {mes_consultado}",
                )
                fig.update_layout(template="plotly_white", height=350)
                return dcc.Graph(id="grafico-pagos-op-semanal", figure=fig)

            if tipo == "banco":
                registros = query_pagos_operativos_por_banco(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay pagos operativos por banco para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                return _crear_tabla_conceptos(
                    "tabla-pagos-op-banco", registros, "Banco"
                )

            if tipo == "cliente":
                registros = query_pagos_operativos_por_cliente(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay pagos operativos por proveedor/cliente para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                return _crear_tabla_conceptos(
                    "tabla-pagos-op-cliente", registros, "Proveedor/Cliente"
                )

            if tipo == "moneda":
                registros = query_pagos_operativos_por_moneda(mes_consultado)
                if not registros:
                    return dbc.Alert(
                        "No hay pagos operativos por moneda para el mes seleccionado.",
                        color="info",
                        className="mb-0",
                    )
                return _crear_tabla_conceptos(
                    "tabla-pagos-op-moneda", registros, "Moneda"
                )

            return dbc.Alert(
                "Seleccione un tipo de detalle válido para pagos operativos.",
                color="warning",
                className="mb-0",
            )

        except Exception:
            return dbc.Alert(
                "Ocurrió un error al obtener el detalle de pagos operativos.",
                color="danger",
                className="mb-0",
            )
