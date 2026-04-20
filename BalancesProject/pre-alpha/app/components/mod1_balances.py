
from dash import dcc, html, dash_table, Input, Output, State, callback_context, no_update
from io import BytesIO
from datetime import datetime
import dash_bootstrap_components as dbc
import pandas as pd

from ..utils import crear_tabla_estilizada_select, _fmt_money
from app.querys import (
    query_bancos,
    query_clasificaciones,
    query_proveedores_clientes,
    query_meses_balance,
    query_mod1,
    update_clasificacion,
    update_proveedor_cliente,
)

# --- Helper: construir tabla con selección ---
def _tabla_balances_component(datos_records):
    columnas = [
        {"name": "ID", "id": "ID"},
        {"name": "Fecha", "id": "Fecha"},
        {"name": "Banco", "id": "Banco"},
        {"name": "Referencia", "id": "Referencia"},
        {"name": "Moneda", "id": "Moneda"},
        {"name": "Descripción", "id": "Concepto"},
        {"name": "Concepto", "id": "Clasificación"},
        {"name": "Proveedor/Cliente", "id": "ProveedorCliente"},
        {"name": "Monto", "id": "Monto"},
    ]

    if not datos_records:
        return crear_tabla_estilizada_select(
            id="tabla-balances",
            columns=columnas,
            data=[],
            page_size=15,
            estilo_adicional=[
                {
                    'if': {'filter_query': '{Fecha} = "TOTAL"'},
                    'backgroundColor': 'rgb(220, 220, 220)',
                    'fontWeight': 'bold'
                }
            ]
        )

    df = pd.DataFrame(datos_records)
    table_data = []
    for _, row in df.iterrows():
        table_data.append({
            "ID": row["id"],
            "Fecha": row["fecha"],
            "Banco": row["banco"],
            "Referencia": row["referencia"],
            "Moneda": row["moneda_ref"],
            "Concepto": row["concepto"],
            "Clasificación": row["clasificacion"],
            "ProveedorCliente": row.get("proveedor_cliente", ""),
            "Monto": _fmt_money(row['monto'])
        })

    monto_total = df["monto"].sum()
    table_data.append({
        "ID": "TOTAL",
        "Fecha": "",
        "Banco": "",
        "Referencia": "",
        "Moneda": "",
        "Concepto": "",
        "Clasificación": "",
        "ProveedorCliente": "",
        "Monto": _fmt_money(monto_total)
    })

    return crear_tabla_estilizada_select(
        id="tabla-balances",
        columns=columnas,
        data=table_data,
        page_size=15,
        estilo_adicional=[
            {
                'if': {'filter_query': '{Fecha} = "TOTAL"'},
                'backgroundColor': 'rgb(220, 220, 220)',
                'fontWeight': 'bold'
            }
        ]
    )

def get_layout():
    return dbc.Container([
        # Título
        html.Div([
            html.H2("Balances Bancarios", className="text-center mb-4 text-primary"),
            html.P("Seleccione los criterios para filtrar el balance general",
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
                            id="input-banco",
                            options=[],
                            placeholder="Seleccione uno o varios bancos",
                            multi=True,
                            className="mt-1"
                        ),
                    ], width=12, lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Concepto", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-clasificacion",
                            options=[],
                            placeholder="Seleccione uno o varios conceptos",
                            multi=True,
                            className="mt-1"
                        ),
                    ], width=12, lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Proveedor/Cliente", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-proveedor",
                            options=[],
                            placeholder="Seleccione uno o varios proveedor/cliente",
                            multi=True,
                            className="mt-1"
                        ),
                    ], width=12, lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Mes", className="fw-bold"),
                        dcc.Dropdown(
                            id="input-mes",
                            options=[],
                            placeholder="Seleccione un mes",
                            multi=False,
                            className="mt-1",
                            clearable=True,
                        ),
                    ], width=12, lg=3, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Generar Reporte", id="btn-generar-reporte", color="primary",
                                   className="w-100 py-2", size="lg"),
                    ], width=12, className="mt-3 text-center"),
                ]),
            ]),
        ], className="mb-4 shadow-sm"),

        # Loader
        dbc.Row([
            dbc.Col([
                dbc.Spinner(
                    html.Div(id="loading-output"),
                    color="primary",
                    type="border",
                    fullscreen=False,
                    size="lg"
                )
            ], width=12)
        ], className="mb-3", id="loader-container", style={"display": "none"}),

        # KPIs
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Total Registros", className="text-center py-2"),
                dbc.CardBody([html.H3("0", id="kpi-total-registros", className="text-center text-primary mb-0")])
            ], color="light", outline=True), width=4, className="mb-3"),
            dbc.Col(dbc.Card([
                dbc.CardHeader("Monto Total", className="text-center py-2"),
                dbc.CardBody([html.H3("$ 0,00", id="kpi-monto-total", className="text-center text-success mb-0")])
            ], color="light", outline=True), width=4, className="mb-3"),
            dbc.Col(dbc.Card([
                dbc.CardHeader("Promedio por Transacción", className="text-center py-2"),
                dbc.CardBody([html.H3("$ 0,00", id="kpi-promedio", className="text-center text-info mb-0")])
            ], color="light", outline=True), width=4, className="mb-3"),
        ], className="mb-4"),

        # Resultados
        dbc.Card([
            dbc.CardHeader("Detalle de Transacciones", className="bg-light py-2"),
            dbc.CardBody([html.Div(_tabla_balances_component([]), id="resultados-balances")]),
        ], id="card-resultados", className="mb-3 shadow-sm", style={"display": "none"}),

        # Editor de Clasificación
        dbc.Card([
            dbc.CardHeader("Editar concepto de la transacción seleccionada", className="bg-light py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Referencia seleccionada", className="fw-bold"),
                        dbc.Input(id="id-seleccionada", type="text", disabled=True, placeholder="Seleccione una fila en la tabla"),
                    ], lg=4, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Usar concepto existente", className="fw-bold"),
                        dcc.Dropdown(
                            id="dropdown-clasificacion-existente",
                            options=[],
                            placeholder="Seleccione un concepto",
                            multi=False,
                            clearable=True
                        ),
                    ], lg=4, className="mb-3"),
                    dbc.Col([
                        dbc.Label("... o ingrese un nuevo concepto", className="fw-bold"),
                        dbc.Input(id="input-clasificacion-nueva", type="text", placeholder="Nuevo concepto"),
                    ], lg=4, className="mb-3"),
                ]),
                dbc.Button("Actualizar concepto", id="btn-actualizar-clasificacion",
                           color="primary", className="mt-2", disabled=True),
                html.Div(id="mensaje-actualizacion", className="mt-3"),
            ]),
        ], id="card-editar-clasificacion", className="mb-4 shadow-sm", style={"display": "none"}),

        # Editor de Proveedor/Cliente
        dbc.Card([
            dbc.CardHeader("Editar proveedor/cliente de la transacción seleccionada", className="bg-light py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Usar proveedor/cliente existente", className="fw-bold"),
                        dcc.Dropdown(
                            id="dropdown-proveedor-existente",
                            options=[],
                            placeholder="Seleccione un proveedor/cliente",
                            multi=False,
                            clearable=True
                        ),
                    ], lg=6, className="mb-3"),
                    dbc.Col([
                        dbc.Label("... o ingrese un nuevo proveedor/cliente", className="fw-bold"),
                        dbc.Input(id="input-proveedor-nuevo", type="text", placeholder="Nuevo proveedor/cliente"),
                    ], lg=6, className="mb-3"),
                ]),
                dbc.Button("Actualizar proveedor/cliente", id="btn-actualizar-proveedor",
                           color="secondary", className="mt-2", disabled=True),
                html.Div(id="mensaje-actualizacion-proveedor", className="mt-3"),
            ]),
        ], id="card-editar-proveedor", className="mb-4 shadow-sm", style={"display": "none"}),

        # Botón de descarga
        dbc.Row([
            dbc.Col([
                dbc.Button("💾 Exportar a Excel", id="btn-exportar-excel",
                           color="success", className="w-100 py-2", style={"display": "none"}),
            ], width=12, className="mt-2"),
        ], id="boton-descarga", className="mb-4"),

        # Componente de descarga
        dcc.Download(id="descargar-excel"),

        # Store
        dcc.Store(id="store-datos-balances"),
    ], fluid=True, className="py-4")


def register_callbacks(app):
    # Inicializar opciones de filtros
    @app.callback(
        [
            Output("input-banco", "options"),
            Output("input-clasificacion", "options"),
            Output("input-proveedor", "options"),
            Output("input-mes", "options"),
        ],
        [Input("btn-generar-reporte", "n_clicks")],
        prevent_initial_call=False
    )
    def inicializar_dropdowns(n_clicks):
        bancos = query_bancos()
        clasificaciones = query_clasificaciones()
        proveedores = query_proveedores_clientes()
        meses = query_meses_balance()
        opciones_banco = [{"label": banco[0], "value": banco[0]} for banco in bancos]
        opciones_clasificacion = [{"label": clas[0], "value": clas[0]} for clas in clasificaciones]
        opciones_proveedor = [{"label": p[0], "value": p[0]} for p in proveedores]
        opciones_mes = [{"label": mes, "value": mes} for mes in meses]
        return opciones_banco, opciones_clasificacion, opciones_proveedor, opciones_mes

    # Generar reporte (KPIs + Store + mostrar editores)
    @app.callback(
        [Output("kpi-total-registros", "children"),
         Output("kpi-monto-total", "children"),
         Output("kpi-promedio", "children"),
         Output("card-resultados", "style"),
         Output("btn-exportar-excel", "style"),
         Output("store-datos-balances", "data"),
         Output("card-editar-clasificacion", "style"),
         Output("card-editar-proveedor", "style")],
        [Input("btn-generar-reporte", "n_clicks")],
        [State("input-banco", "value"),
         State("input-clasificacion", "value"),
         State("input-proveedor", "value"),
         State("input-mes", "value")]
    )
    def generar_reporte_balances(
        n_clicks,
        bancos_seleccionados,
        clasificaciones_seleccionadas,
        proveedores_seleccionados,
        mes_seleccionado,
    ):
        if n_clicks is None:
            return "0", "$ 0,00", "$ 0,00", {"display": "none"}, {"display": "none"}, {}, {"display": "none"}, {"display": "none"}
        try:
            data = query_mod1(
                bancos_seleccionados,
                clasificaciones_seleccionadas,
                proveedores_seleccionados,
                mes_seleccionado,
            )
            if not data:
                return ("0", "$ 0,00", "$ 0,00",
                        {"display": "block"}, {"display": "none"}, {}, {"display": "none"}, {"display": "none"})

            df = pd.DataFrame(data, columns=['id','fecha', 'banco', 'referencia', 'moneda_ref', 'concepto', 'clasificacion', 'proveedor_cliente', 'monto'])

            df.sort_values(by=['fecha','banco'], ascending=[True, False], inplace=True)


            total_registros = len(df)
            monto_total = df['monto'].sum()
            promedio = monto_total / total_registros if total_registros > 0 else 0

            kpi_registros = f"{total_registros:,}".replace(",", ".")
            kpi_monto_total = _fmt_money(monto_total)
            kpi_promedio = _fmt_money(promedio)

            return (kpi_registros, kpi_monto_total, kpi_promedio,
                    {"display": "block"}, {"display": "block"},
                    df.to_dict("records"),
                    {"display": "block"}, {"display": "block"})
        except Exception as e:
            return "0", "$ 0,00", "$ 0,00", {"display": "block"}, {"display": "none"}, {}, {"display": "none"}, {"display": "none"}

    # Refrescar tabla
    @app.callback(
        Output("resultados-balances", "children"),
        [Input("store-datos-balances", "data")]
    )
    def refrescar_tabla(datos):
        if not datos:
            return _tabla_balances_component([])
        return _tabla_balances_component(datos)

    # Editor de Clasificación: opciones y habilitación
    @app.callback(
        [Output("id-seleccionada", "value"),
         Output("dropdown-clasificacion-existente", "options"),
         Output("btn-actualizar-clasificacion", "disabled")],
        [Input("tabla-balances", "selected_rows"),
         Input("store-datos-balances", "data")],
        [State("tabla-balances", "data")],
        prevent_initial_call=False
    )
    def actualizar_editor_clas(selected_rows, datos_store, tabla_data):
        clasificaciones = query_clasificaciones()
        opciones_clas = [{"label": c[0], "value": c[0]} for c in clasificaciones]

        if not tabla_data or not selected_rows:
            return "", opciones_clas, True

        idx = selected_rows[0]
        row = tabla_data[idx]
        if row.get("Fecha") == "TOTAL":
            return "", opciones_clas, True

        id = row.get("ID", "")
        return id, opciones_clas, (id == "")

    # Editor de Proveedor/Cliente: opciones y habilitación
    @app.callback(
        [Output("dropdown-proveedor-existente", "options"),
         Output("btn-actualizar-proveedor", "disabled")],
        [Input("tabla-balances", "selected_rows"),
         Input("store-datos-balances", "data")],
        [State("tabla-balances", "data")],
        prevent_initial_call=False
    )
    def actualizar_editor_prov(selected_rows, datos_store, tabla_data):
        proveedores = query_proveedores_clientes()
        opciones_prov = [{"label": p[0], "value": p[0]} for p in proveedores]

        if not tabla_data or not selected_rows:
            return opciones_prov, True

        idx = selected_rows[0]
        row = tabla_data[idx]
        if row.get("Fecha") == "TOTAL":
            return opciones_prov, True

        id = row.get("ID", "")
        return opciones_prov, (id == "")

    # Actualizar Clasificación (BD + Store)
    @app.callback(
        [Output("mensaje-actualizacion", "children"),
         Output("store-datos-balances", "data", allow_duplicate=True)],
        [Input("btn-actualizar-clasificacion", "n_clicks")],
        [State("id-seleccionada", "value"),
         State("dropdown-clasificacion-existente", "value"),
         State("input-clasificacion-nueva", "value"),
         State("store-datos-balances", "data")],
        prevent_initial_call=True
    )
    def actualizar_clasificacion_bd(n_clicks, id, clas_existente, clas_nueva, datos):
        if not n_clicks:
            return no_update, no_update

        nueva = (clas_nueva or "").strip() if clas_nueva else (clas_existente or "")
        if not id or not nueva:
            return (dbc.Alert("Debe seleccionar una fila y definir una clasificación.", color="warning"), no_update)

        try:
            afectadas = update_clasificacion(id, nueva)
            if afectadas == 0:
                return (dbc.Alert("No se encontraron filas para actualizar con ese ID.", color="warning"), no_update)

            df = pd.DataFrame(datos)
            df.loc[df["id"] == id, "clasificacion"] = nueva
            msg = dbc.Alert(f"Clasificación actualizada con éxito a «{nueva}».", color="success")
            return msg, df.to_dict("records")
        except Exception as e:
            return (dbc.Alert(f"Error al actualizar: {str(e)}", color="danger"), no_update)

    # Actualizar Proveedor/Cliente (BD + Store)
    @app.callback(
        [Output("mensaje-actualizacion-proveedor", "children"),
         Output("store-datos-balances", "data", allow_duplicate=True)],
        [Input("btn-actualizar-proveedor", "n_clicks")],
        [State("id-seleccionada", "value"),
         State("dropdown-proveedor-existente", "value"),
         State("input-proveedor-nuevo", "value"),
         State("store-datos-balances", "data")],
        prevent_initial_call=True
    )
    def actualizar_proveedor_bd(n_clicks, id, prov_existente, prov_nuevo, datos):
        if not n_clicks:
            return no_update, no_update

        nuevo = (prov_nuevo or "").strip() if prov_nuevo else (prov_existente or "")
        if not id or not nuevo:
            return (dbc.Alert("Debe seleccionar una fila y definir un proveedor/cliente.", color="warning"), no_update)

        try:
            afectadas = update_proveedor_cliente(id, nuevo)
            if afectadas == 0:
                return (dbc.Alert("No se encontraron filas para actualizar con ese ID.", color="warning"), no_update)

            df = pd.DataFrame(datos)
            df.loc[df["id"] == id, "proveedor_cliente"] = nuevo
            msg = dbc.Alert(f"Proveedor/Cliente actualizado con éxito a «{nuevo}».", color="success")
            return msg, df.to_dict("records")
        except Exception as e:
            return (dbc.Alert(f"Error al actualizar: {str(e)}", color="danger"), no_update)

    # Exportar a Excel
    @app.callback(
        Output("descargar-excel", "data"),
        [Input("btn-exportar-excel", "n_clicks")],
        [State("store-datos-balances", "data")]
    )
    def exportar_excel(n_clicks, datos):
        if n_clicks is None:
            return None
        try:
            df = pd.DataFrame(datos)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name="Balances Bancarios", index=False)
            output.seek(0)
            return dcc.send_bytes(
                output.getvalue(),
                filename=f"balances_bancarios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
        except Exception as e:
            return (dbc.Alert(f"Error al exportar a Excel: {str(e)}", color="danger"), no_update)
