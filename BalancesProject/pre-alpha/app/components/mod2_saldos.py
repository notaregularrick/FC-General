from dash import dcc, html, Input, Output, State, callback_context, no_update
from io import BytesIO
from datetime import datetime
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from app.utils import crear_tabla_estilizada, _fmt_money, MESES_INV
from app.querys import (
    query_saldos_finales, 
    query_saldos_iniciales_semana, 
    query_masa_monetaria,
    query_meses_disponibles,
    query_semanas_disponibles
)

def get_layout():
    return dbc.Container([
        # Título
        html.Div([
            html.H2("Disponibilidad bancaria", className="text-center mb-4 text-primary"),
            html.P("Muestra los saldos iniciales y finales de cada banco por semana y el saldo total",
                   className="text-center text-muted mb-4")
        ], className="mb-4"),


        # FILTROS DE MES Y SEMANA
        dbc.Card([
            dbc.CardHeader("Filtros", className="bg-primary text-white py-2"),
            
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Mes", className="fw-bold"),
                        dcc.Dropdown(
                            id="filtro-mes-saldos",
                            placeholder="Seleccione un mes...",
                            clearable=True,
                            className="mb-2"
                        )
                    ], width=12, md=6),
                    dbc.Col([
                        dbc.Label("Semana", className="fw-bold"),
                        dcc.Dropdown(
                            id="filtro-semana-saldos",
                            placeholder="Seleccione una semana...",
                            clearable=True,
                            className="mb-2"
                        )
                    ], width=12, md=6),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("🔍 Consultar", id="btn-consultar-saldos", 
                                   color="primary", className="w-100 mt-2")
                    ], width=12, md=4),
                    dbc.Col([
                        dbc.Button("🔄 Limpiar Filtros", id="btn-limpiar-filtros-saldos", 
                                   color="outline-secondary", className="w-100 mt-2")
                    ], width=12, md=4),
                ], justify="center")
            ])
        ], className="mb-4 shadow-sm"),

        # Contenido principal: izquierda = Iniciales, derecha = Finales y KPIs
        dbc.Row([
            # ——— COLUMNA IZQUIERDA: Saldos Iniciales de la Semana ———
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Saldos Iniciales de la Semana (Bolívares)", className="bg-light py-2"),
                    dbc.CardBody([
                        html.Div(id="tabla-saldos-iniciales-bs-container"),
                    ]),
                ], className="mb-3 shadow-sm"),
                dbc.Card([
                    dbc.CardHeader("Saldos Iniciales de la Semana (Dólares)", className="bg-light py-2"),
                    dbc.CardBody([
                        html.Div(id="tabla-saldos-iniciales-us-container"),
                    ]),
                ], className="mb-3 shadow-sm"),
            ], width=12, lg=6),

            # ——— COLUMNA DERECHA: Saldo a final de semana + Saldos Finales ———
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Saldo a Final de Semana Total",
                                 className="text-center py-3 bg-secondary text-white"),
                    dbc.CardBody([
                        html.H2("$ 0,00", id="kpi-masa-monetaria-total",
                                className="text-center text-white mb-0 display-6")
                    ], className="bg-secondary")
                ], className="mb-3"),
                dbc.Card([
                    dbc.CardHeader("Saldo a Final de Semana (Bolívares)",
                                 className="text-center py-3 bg-success text-white"),
                    dbc.CardBody([
                        html.H2("$ 0,00", id="kpi-masa-monetaria-bs",
                                className="text-center text-white mb-0 display-6")
                    ], className="bg-success")
                ], className="border-0 shadow-lg mb-3"),
                dbc.Card([
                    dbc.CardHeader("Saldo a Final de Semana (Dólares)",
                                 className="text-center py-3 bg-primary text-white"),
                    dbc.CardBody([
                        html.H2("$ 0,00", id="kpi-masa-monetaria-us",
                                className="text-center text-white mb-0 display-6")
                    ], className="bg-primary")
                ], className="border-0 shadow-lg mb-3"),
                dbc.Card([
                    dbc.CardHeader("Saldos Finales de la Semana (Bolívares)", className="bg-light py-2"),
                    dbc.CardBody([
                        html.Div(id="tabla-saldos-finales-bs-container"),
                    ]),
                ], id="card-resultados-saldos-bs", className="mb-3 shadow-sm"),
                dbc.Card([
                    dbc.CardHeader("Saldos Finales de la Semana (Dólares)", className="bg-light py-2"),
                    dbc.CardBody([
                        html.Div(id="tabla-saldos-finales-us-container"),
                    ]),
                ], id="card-resultados-saldos-us", className="mb-3 shadow-sm"),
            ], width=12, lg=6),
        ], className="mb-4"),

        # Gráficos de dona (distribución porcentual) — debajo del bloque principal
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Distribución Porcentual por Banco (Bolívares)", className="bg-light py-2"),
                    dbc.CardBody([
                        dcc.Graph(id="grafico-dona-saldos-bs", style={"height": "380px"})
                    ])
                ], className="shadow-sm h-100")
            ], width=12, lg=6, className="mb-4"),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Distribución Porcentual por Banco (Dólares)", className="bg-light py-2"),
                    dbc.CardBody([
                        dcc.Graph(id="grafico-dona-saldos-us", style={"height": "380px"})
                    ])
                ], className="shadow-sm h-100")
            ], width=12, lg=6, className="mb-4"),
        ]),
        
        # Botón de descarga
        dbc.Row([
            dbc.Col([
                dbc.Button("💾 Exportar a Excel (BS origen)", id="btn-exportar-excel-saldos-bs",
                           color="success", className="w-100 py-2", style={"display": "none"}),
            ], width=6, className="mt-2"),
            dbc.Col([
                dbc.Button("💾 Exportar a Excel (US)", id="btn-exportar-excel-saldos-us",
                           color="primary", className="w-100 py-2", style={"display": "none"}),
            ], width=6, className="mt-2"),
        ], id="boton-descarga-saldos", className="mb-4"),

        # Componentes de descarga
        dcc.Download(id="descargar-excel-saldos-bs"),
        dcc.Download(id="descargar-excel-saldos-us"),

        # Almacenamiento para datos temporales (uno por moneda)
        dcc.Store(id="store-datos-saldos-bs"),
        dcc.Store(id="store-datos-saldos-us"),
        
        # Interval para cargar datos automáticamente
        dcc.Interval(id='interval-carga-inicial-saldos', interval=1000, n_intervals=0, max_intervals=1)
    ], fluid=True, className="py-4")

def register_callbacks(app):
    # Callback para cargar los meses disponibles al iniciar
    @app.callback(
        Output("filtro-mes-saldos", "options"),
        [Input('interval-carga-inicial-saldos', 'n_intervals')],
        prevent_initial_call=False
    )
    def cargar_meses_disponibles(n_intervals):
        try:
            meses = query_meses_disponibles()
            return [{"label": mes, "value": mes} for mes in meses]
        except Exception as e:
            print(f"Error cargando meses: {e}")
            return []

    # Callback para actualizar semanas disponibles según el mes seleccionado
    @app.callback(
        Output("filtro-semana-saldos", "options"),
        [Input("filtro-mes-saldos", "value"),
         Input('interval-carga-inicial-saldos', 'n_intervals')],
        prevent_initial_call=False
    )
    def actualizar_semanas_disponibles(mes_seleccionado, n_intervals):
        if mes_seleccionado is None:
            return []
        try:
            semanas = query_semanas_disponibles(mes_seleccionado)
            return [{"label": f"Semana {s}", "value": s} for s in semanas]
        except Exception as e:
            print(f"Error cargando semanas: {e}")
            return []

    # Callback para limpiar filtros
    @app.callback(
        [Output("filtro-mes-saldos", "value"),
         Output("filtro-semana-saldos", "value")],
        [Input("btn-limpiar-filtros-saldos", "n_clicks")],
        prevent_initial_call=True
    )
    def limpiar_filtros(n_clicks):
        if n_clicks:
            return None, None
        return no_update, no_update

    # Callback para cargar los datos (BOLÍVARES - BS)
    @app.callback(
        [Output("tabla-saldos-iniciales-bs-container", "children"),
         Output("tabla-saldos-finales-bs-container", "children"),
         Output("kpi-masa-monetaria-bs", "children"),
         Output("grafico-dona-saldos-bs", "figure"),
         Output("card-resultados-saldos-bs", "style"),
         Output("btn-exportar-excel-saldos-bs", "style"),
         Output("store-datos-saldos-bs", "data")],
        [Input("btn-consultar-saldos", "n_clicks"),
         Input('interval-carga-inicial-saldos', 'n_intervals')],
        [State("filtro-mes-saldos", "value"),
         State("filtro-semana-saldos", "value")]
    )
    def cargar_saldos_bs(n_clicks, n_intervals, mes, semana):
        return _cargar_saldos_por_moneda(n_clicks, n_intervals, "BS", "tabla-saldos-bs", mes, semana)

    # Callback para cargar los datos (DÓLARES - US)
    @app.callback(
        [Output("tabla-saldos-iniciales-us-container", "children"),
         Output("tabla-saldos-finales-us-container", "children"),
         Output("kpi-masa-monetaria-us", "children"),
         Output("grafico-dona-saldos-us", "figure"),
         Output("card-resultados-saldos-us", "style"),
         Output("btn-exportar-excel-saldos-us", "style"),
         Output("store-datos-saldos-us", "data")],
        [Input("btn-consultar-saldos", "n_clicks"),
         Input('interval-carga-inicial-saldos', 'n_intervals')],
        [State("filtro-mes-saldos", "value"),
         State("filtro-semana-saldos", "value")]
    )
    def cargar_saldos_us(n_clicks, n_intervals, mes, semana):
        return _cargar_saldos_por_moneda(n_clicks, n_intervals, "US", "tabla-saldos-us", mes, semana)

    # Callback para exportar a Excel (BOLÍVARES)
    @app.callback(
        Output("descargar-excel-saldos-bs", "data"),
        [Input("btn-exportar-excel-saldos-bs", "n_clicks")],
        [State("store-datos-saldos-bs", "data")]
    )
    def exportar_excel_saldos_bs(n_clicks, datos):
        return _exportar_excel(n_clicks, datos, "saldos_finales_bs")

    # Callback para exportar a Excel (DÓLARES)
    @app.callback(
        Output("descargar-excel-saldos-us", "data"),
        [Input("btn-exportar-excel-saldos-us", "n_clicks")],
        [State("store-datos-saldos-us", "data")]
    )
    def exportar_excel_saldos_us(n_clicks, datos):
        return _exportar_excel(n_clicks, datos, "saldos_finales_us")

    # Función auxiliar para calcular el saldo total de la semana
    @app.callback(
        Output("kpi-masa-monetaria-total", "children"),
        [Input("store-datos-saldos-bs", "data"),
         Input("store-datos-saldos-us", "data")]
    )           
    def _calcular_saldo_total(datos_bs, datos_us):
        #reemplazar los none de la listas por cero
        datos_bs = [{k: v if v is not None else 0 for k, v in row.items()} for row in datos_bs]
        datos_us = [{k: v if v is not None else 0 for k, v in row.items()} for row in datos_us]
        saldo_total_bs = sum(row['Saldo_Final'] for row in datos_bs if row['moneda_ref'] == 'BS')
        saldo_total_us = sum(row['Saldo_Final'] for row in datos_us if row['moneda_ref'] == 'US')
        return _fmt_money(saldo_total_bs + saldo_total_us)


def _cargar_saldos_por_moneda(n_clicks, n_intervals, moneda_ref: str, tabla_id_prefix: str, mes: str = None, semana: int = None):
    """Función auxiliar para cargar saldos por moneda específica con filtros de mes y semana."""
    fig_vacia = {}
    valor_default = _fmt_money(0, moneda_ref)

    # Solo cargar si es el interval inicial o si se hizo click en consultar
    if (n_intervals is None or n_intervals == 0) and n_clicks is None:
        return None, None, valor_default, fig_vacia, {"display": "none"}, {"display": "none"}, {}
    
    try:
        # Obtener datos de la base de datos con los filtros
        data_finales = query_saldos_finales(moneda_ref, mes, semana)
        data_iniciales = query_saldos_iniciales_semana(moneda_ref, mes, semana)
        
        if not data_finales:
            filtro_info = f"({moneda_ref}"
            if mes:
                filtro_info += f", Mes: {mes}"
            if semana:
                filtro_info += f", Semana: {semana}"
            filtro_info += ")"
            return (
                dbc.Alert(f"No se encontraron datos de saldos iniciales {filtro_info}", color="warning"),
                dbc.Alert(f"No se encontraron datos de saldos finales {filtro_info}", color="warning"),
                valor_default, 
                fig_vacia, 
                {"display": "block"}, 
                {"display": "none"}, 
                {}
            )
        
        # Convertir a DataFrame
        df_finales = pd.DataFrame(data_finales, columns=['banco', 'Saldo_Final', 'fecha', 'semana', 'moneda_ref'])
        df_iniciales = pd.DataFrame(data_iniciales, columns=['banco', 'Saldo_Inicial', 'fecha', 'semana']) if data_iniciales else pd.DataFrame()
        
        # Calcular KPI con filtros
        masa_monetaria = query_masa_monetaria(moneda_ref, mes, semana)
        kpi_masa = _fmt_money(masa_monetaria, moneda_ref) if masa_monetaria else valor_default
        
        # ---------------------------------------------------------
        # CREACIÓN DEL GRÁFICO DE DONA
        # ---------------------------------------------------------
        colores = px.colors.qualitative.Prism if moneda_ref == "BS" else px.colors.qualitative.Bold
        
        # Ordenar por saldo descendente para mejorar distribución de etiquetas (mayores primero)
        df_pie = df_finales.sort_values('Saldo_Final', ascending=False).reset_index(drop=True)
        
        fig = px.pie(df_pie, 
                     values='Saldo_Final', 
                     names='banco', 
                     hole=0.5,
                     color_discrete_sequence=colores)
        
        # Nombre + porcentaje fuera del gráfico; sin leyenda; ocultar texto en porciones muy pequeñas
        fig.update_traces(textposition='outside', textinfo='percent+label', textfont_size=11)
        fig.update_layout(
            margin=dict(t=40, b=40, l=40, r=40),
            showlegend=False,
            uniformtext=dict(minsize=8, mode="hide"),
        )
        # ---------------------------------------------------------

        # Preparar datos para la tabla de saldos finales
        table_data = []
        for _, row in df_finales.iterrows():
            table_data.append({
                "Banco": row["banco"],
                "Mes": row["fecha"],
                "Semana": row["semana"],
                "Saldo Final": _fmt_money(row['Saldo_Final'], moneda_ref)
            })
        
        tabla_saldos_finales = crear_tabla_estilizada(
            id=f"{tabla_id_prefix}-finales",
            columns=[
                {"name": "Banco", "id": "Banco"},
                {"name": "Mes", "id": "Mes"},
                {"name": "Semana", "id": "Semana"},
                {"name": "Saldo Final", "id": "Saldo Final"}
            ],
            data=table_data,
            estilo_adicional=[{'if': {'filter_query': '{Banco} = "TOTAL"'}, 'backgroundColor': 'rgb(220, 220, 220)', 'fontWeight': 'bold'}]
        )

        # Preparar datos para la tabla de saldos iniciales de la semana
        tabla_saldos_iniciales = None
        if not df_iniciales.empty:
            table_data_iniciales = []
            for _, row in df_iniciales.iterrows():
                table_data_iniciales.append({
                    "Banco": row["banco"],
                    "Mes": row["fecha"],
                    "Semana": row["semana"],
                    "Saldo Inicial": _fmt_money(row['Saldo_Inicial'], moneda_ref)
                })

            tabla_saldos_iniciales = crear_tabla_estilizada(
                id=f"{tabla_id_prefix}-iniciales",
                columns=[
                    {"name": "Banco", "id": "Banco"},
                    {"name": "Mes", "id": "Mes"},
                    {"name": "Semana", "id": "Semana"},
                    {"name": "Saldo Inicial", "id": "Saldo Inicial"}
                ],
                data=table_data_iniciales,
                estilo_adicional=[{'if': {'filter_query': '{Banco} = "TOTAL"'}, 'backgroundColor': 'rgb(220, 220, 220)', 'fontWeight': 'bold'}]
            )
        else:
            tabla_saldos_iniciales = dbc.Alert(f"No hay saldos iniciales para {moneda_ref}", color="info")

        return [
            tabla_saldos_iniciales,
            tabla_saldos_finales,
            kpi_masa,
            fig,
            {"display": "block"},
            {"display": "block"},
            df_finales.to_dict("records")
        ]
        
    except Exception as e:
        return (
            dbc.Alert(f"Error: {str(e)}", color="danger"),
            dbc.Alert(f"Error: {str(e)}", color="danger"),
            valor_default, 
            fig_vacia, 
            {"display": "block"}, 
            {"display": "none"}, 
            {}
        )




def _exportar_excel(n_clicks, datos, filename_prefix: str):
    """Función auxiliar para exportar datos a Excel."""
    if n_clicks is None:
        return None
    
    try:
        df = pd.DataFrame(datos)
        total_fila = pd.DataFrame({'banco': ['TOTAL'], 'Saldo_Final': [df['Saldo_Final'].sum()]})
        df_con_total = pd.concat([df, total_fila], ignore_index=True)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_con_total.to_excel(writer, sheet_name="Saldos Finales", index=False)
        output.seek(0)
        return dcc.send_bytes(output.getvalue(), filename=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
    except Exception as e:
        return None