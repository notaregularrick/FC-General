from dash import dcc, html, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import pandas as pd
from io import BytesIO
from datetime import datetime

from ..utils import crear_tabla_estilizada, _fmt_money
from app.querys import (
    query_bancos,
    query_meses_balance,
    query_ingresos_agrupados,
    query_egresos_agrupados,
    query_flujo_caja_agrupado,
)

try:
    # Librerías para generación de PDF (agregar a requirements.txt si no están)
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


def _tabla_agrupada_component(table_id, titulo, data_records, col_concepto, col_importe):
    """
    Construye una tabla estilizada con totales al final.
    """
    columnas = [
        {"name": "Concepto", "id": "Concepto"},
        {"name": "Importe", "id": "Importe"},
    ]

    if not data_records:
        return crear_tabla_estilizada(
            id=table_id,
            columns=columnas,
            data=[],
            page_size=15,
            estilo_adicional=[]
        )

    df = pd.DataFrame(data_records, columns=[col_concepto, col_importe])

    table_data = []
    for _, row in df.iterrows():
        table_data.append({
            "Concepto": row[col_concepto],
            "Importe": _fmt_money(row[col_importe]),
        })

    total = df[col_importe].sum()
    table_data.append({
        "Concepto": "TOTAL",
        "Importe": _fmt_money(total),
    })

    return crear_tabla_estilizada(
        id=table_id,
        columns=columnas,
        data=table_data,
        page_size=2000,
        estilo_adicional=[
            {
                "if": {"filter_query": '{Concepto} = "TOTAL"'},
                "backgroundColor": "rgb(220, 220, 220)",
                "fontWeight": "bold",
            }
        ],
    )

def get_layout():
    return dbc.Container(
        [
            # Título
            html.Div(
                [
                    html.H2(
                        "Resumen de Ingresos, Egresos y Flujo de Caja",
                        className="text-center mb-4 text-primary",
                    ),
                    html.P(
                        "Filtre por banco y mes para ver los ingresos, egresos y el flujo de caja.",
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
                                            dbc.Label("Banco", className="fw-bold"),
                                            dcc.Dropdown(
                                                id="input-banco-flujo",
                                                options=[],
                                                placeholder="Seleccione uno o varios bancos",
                                                multi=True,
                                                className="mt-1",
                                            ),
                                        ],
                                        width=12,
                                        lg=6,
                                        className="mb-3",
                                    ),
                                    dbc.Col(
                                        [
                                            dbc.Label("Mes", className="fw-bold"),
                                            dcc.Dropdown(
                                                id="input-mes-flujo",
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
                                                "Generar Reporte",
                                                id="btn-generar-flujo",
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

            # Resultados: Ingresos, Egresos, Flujo de Caja
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Ingresos por Clasificación",
                                    className="bg-light py-2",
                                ),
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            _tabla_agrupada_component(
                                                "tabla-ingresos",
                                                "Ingresos",
                                                [],
                                                "clasificacion",
                                                "importe",
                                            ),
                                            id="tabla-ingresos-container",
                                        )
                                    ]
                                ),
                            ],
                            className="mb-3 shadow-sm",
                        ),
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Egresos por Clasificación",
                                    className="bg-light py-2",
                                ),
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            _tabla_agrupada_component(
                                                "tabla-egresos",
                                                "Egresos",
                                                [],
                                                "clasificacion",
                                                "importe",
                                            ),
                                            id="tabla-egresos-container",
                                        )
                                    ]
                                ),
                            ],
                            className="mb-3 shadow-sm",
                        )
                    )],
                className="mb-4",
            ),

            # Flujo de Caja
            dbc.Row([
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
                                            _tabla_agrupada_component(
                                                "tabla-flujo-caja",
                                                "Flujo de Caja",
                                                [],
                                                "concepto_gen",
                                                "importe",
                                            ),
                                            id="tabla-flujo-caja-container",
                                        )
                                    ]
                                ),
                            ],
                            className="mb-3 shadow-sm",
                        )
                    ),
            ]),


            # Botones de exportación
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button(
                            "💾 Exportar a Excel",
                            id="btn-exportar-excel-flujo",
                            color="success",
                            className="w-100 py-2",
                            style={"display": "none"},
                        ),
                        width=12,
                        lg=6,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Button(
                            "🧾 Exportar a PDF",
                            id="btn-exportar-pdf-flujo",
                            color="danger",
                            className="w-100 py-2",
                            style={"display": "none"},
                        ),
                        width=12,
                        lg=6,
                        className="mb-3",
                    ),
                ],
                className="mb-4",
            ),

            # Componentes de descarga
            dcc.Download(id="descargar-excel-flujo"),
            dcc.Download(id="descargar-pdf-flujo"),

            # Store con los datos de las tres tablas
            dcc.Store(id="store-datos-flujo"),
        ],
        fluid=True,
        className="py-4",
    )


def register_callbacks(app):
    # Inicializar opciones de filtros
    @app.callback(
        [
            Output("input-banco-flujo", "options"),
            Output("input-mes-flujo", "options"),
        ],
        [Input("btn-generar-flujo", "n_clicks")],
        prevent_initial_call=False,
    )
    def inicializar_dropdowns_flujo(n_clicks):
        bancos = query_bancos()
        meses = query_meses_balance()

        opciones_banco = [{"label": banco[0], "value": banco[0]} for banco in bancos]
        opciones_mes = [{"label": mes, "value": mes} for mes in meses]

        return opciones_banco, opciones_mes

    # Generar reporte (llenar tablas y habilitar exportación)
    @app.callback(
        [
            Output("tabla-ingresos-container", "children"),
            Output("tabla-egresos-container", "children"),
            Output("tabla-flujo-caja-container", "children"),
            Output("store-datos-flujo", "data"),
            Output("btn-exportar-excel-flujo", "style"),
            Output("btn-exportar-pdf-flujo", "style"),
        ],
        [Input("btn-generar-flujo", "n_clicks")],
        [
            State("input-banco-flujo", "value"),
            State("input-mes-flujo", "value"),
        ],
    )
    def generar_reporte_flujo(n_clicks, bancos_seleccionados, mes_seleccionado):
        if n_clicks is None:
            tabla_vacia_ing = _tabla_agrupada_component(
                "tabla-ingresos", "Ingresos", [], "clasificacion", "importe"
            )
            tabla_vacia_egr = _tabla_agrupada_component(
                "tabla-egresos", "Egresos", [], "clasificacion", "importe"
            )
            tabla_vacia_flujo = _tabla_agrupada_component(
                "tabla-flujo-caja", "Flujo de Caja", [], "concepto_gen", "importe"
            )
            return (
                tabla_vacia_ing,
                tabla_vacia_egr,
                tabla_vacia_flujo,
                {},
                {"display": "none"},
                {"display": "none"},
            )

        try:
            ingresos = query_ingresos_agrupados(bancos_seleccionados, mes_seleccionado)
            egresos = query_egresos_agrupados(bancos_seleccionados, mes_seleccionado)
            flujo = query_flujo_caja_agrupado(bancos_seleccionados, mes_seleccionado)

            df_ing = pd.DataFrame(ingresos, columns=["clasificacion", "importe"])
            df_egr = pd.DataFrame(egresos, columns=["clasificacion", "importe"])
            df_flujo = pd.DataFrame(flujo, columns=["concepto_gen", "importe"])

            tabla_ing = _tabla_agrupada_component(
                "tabla-ingresos",
                "Ingresos",
                df_ing.to_dict("records"),
                "clasificacion",
                "importe",
            )
            tabla_egr = _tabla_agrupada_component(
                "tabla-egresos",
                "Egresos",
                df_egr.to_dict("records"),
                "clasificacion",
                "importe",
            )
            tabla_flujo = _tabla_agrupada_component(
                "tabla-flujo-caja",
                "Flujo de Caja",
                df_flujo.to_dict("records"),
                "concepto_gen",
                "importe",
            )

            hay_datos = not (
                df_ing.empty and df_egr.empty and df_flujo.empty
            )
            style_btn = {"display": "block"} if hay_datos else {"display": "none"}

            store_data = {
                "ingresos": df_ing.to_dict("records"),
                "egresos": df_egr.to_dict("records"),
                "flujo": df_flujo.to_dict("records"),
            }

            return (
                tabla_ing,
                tabla_egr,
                tabla_flujo,
                store_data,
                style_btn,
                style_btn,
            )
        except Exception:
            tabla_vacia_ing = _tabla_agrupada_component(
                "tabla-ingresos", "Ingresos", [], "clasificacion", "importe"
            )
            tabla_vacia_egr = _tabla_agrupada_component(
                "tabla-egresos", "Egresos", [], "clasificacion", "importe"
            )
            tabla_vacia_flujo = _tabla_agrupada_component(
                "tabla-flujo-caja", "Flujo de Caja", [], "concepto_gen", "importe"
            )
            return (
                tabla_vacia_ing,
                tabla_vacia_egr,
                tabla_vacia_flujo,
                {},
                {"display": "none"},
                {"display": "none"},
            )

    # Exportar a Excel (3 hojas: Ingresos, Egresos, Flujo de Caja)
    @app.callback(
        Output("descargar-excel-flujo", "data"),
        [Input("btn-exportar-excel-flujo", "n_clicks")],
        [State("store-datos-flujo", "data")],
        prevent_initial_call=True,
    )
    def exportar_excel_flujo(n_clicks, datos):
        if not n_clicks or not datos:
            return None
        try:
            df_ing = pd.DataFrame(datos.get("ingresos", []))
            df_egr = pd.DataFrame(datos.get("egresos", []))
            df_flujo = pd.DataFrame(datos.get("flujo", []))

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                if not df_ing.empty:
                    df_ing.to_excel(writer, sheet_name="Ingresos", index=False)
                if not df_egr.empty:
                    df_egr.to_excel(writer, sheet_name="Egresos", index=False)
                if not df_flujo.empty:
                    df_flujo.to_excel(writer, sheet_name="Flujo_Caja", index=False)

            output.seek(0)
            return dcc.send_bytes(
                output.getvalue(),
                filename=f"flujo_caja_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            )
        except Exception:
            return None

    # Exportar a PDF (tablas simples de cada resumen)
    @app.callback(
        Output("descargar-pdf-flujo", "data"),
        [Input("btn-exportar-pdf-flujo", "n_clicks")],
        [State("store-datos-flujo", "data")],
        prevent_initial_call=True,
    )
    def exportar_pdf_flujo(n_clicks, datos):
        if not n_clicks or not datos or not REPORTLAB_AVAILABLE:
            return None

        try:
            df_ing = pd.DataFrame(datos.get("ingresos", []))
            df_egr = pd.DataFrame(datos.get("egresos", []))
            df_flujo = pd.DataFrame(datos.get("flujo", []))

            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter)
            elements = []
            styles = getSampleStyleSheet()

            def _add_df_to_pdf(df, titulo):
                if df.empty:
                    return
                elements.append(Paragraph(titulo, styles["Heading2"]))
                data_table = [list(df.columns)] + df.values.tolist()
                table = Table(data_table)
                table.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                            ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                        ]
                    )
                )
                elements.append(table)
                elements.append(Spacer(1, 12))

            _add_df_to_pdf(df_ing, "Ingresos por Clasificación")
            _add_df_to_pdf(df_egr, "Egresos por Clasificación")
            _add_df_to_pdf(df_flujo, "Flujo de Caja por Concepto General")

            if not elements:
                return None

            doc.build(elements)
            pdf_bytes = buffer.getvalue()
            buffer.close()

            return dcc.send_bytes(
                pdf_bytes,
                filename=f"flujo_caja_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            )
        except Exception:
            return None
