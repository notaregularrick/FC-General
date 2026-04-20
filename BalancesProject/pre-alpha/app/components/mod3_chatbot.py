
# modules/mod3_chatbot.py
from dash import dcc, html, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import pandas as pd
from app.querys import (
    query_bancos,
    query_clasificaciones,
    query_proveedores_clientes,
    query_meses_balance,
    query_mod1,
)
from app.chat import responder_con_contexto, construir_contexto_mod1  
from ..utils import _fmt_money

def get_layout():
    return dbc.Container([
        html.Div([
            html.H2("Asistente (Chatbot) - Balance General", className="text-center mb-3 text-primary"),
            html.P("Usa los filtros para establecer el contexto del chat con datos del balance general.",
                   className="text-center text-muted mb-4")
        ]),

        # Filtros de contexto (iguales que Mod1)
        dbc.Card([
            dbc.CardHeader("Contexto del Chat", className="bg-primary text-white py-2"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Banco", className="fw-bold"),
                        dcc.Dropdown(id="mod3-input-banco", options=[], multi=True,
                                     placeholder="Seleccione uno o varios bancos", className="mt-1"),
                    ], lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Clasificación", className="fw-bold"),
                        dcc.Dropdown(id="mod3-input-clasificacion", options=[], multi=True,
                                     placeholder="Seleccione una o varias clasificaciones", className="mt-1"),
                    ], lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Proveedor/Cliente", className="fw-bold"),
                        dcc.Dropdown(id="mod3-input-proveedor", options=[], multi=True,
                                     placeholder="Seleccione uno o varios proveedor/cliente", className="mt-1"),
                    ], lg=3, className="mb-3"),
                    dbc.Col([
                        dbc.Label("Mes", className="fw-bold"),
                        dcc.Dropdown(
                            id="mod3-input-mes",
                            options=[],
                            multi=False,
                            placeholder="Seleccione un mes",
                            className="mt-1",
                            clearable=True,
                        ),
                    ], lg=3, className="mb-3"),
                ]),
                dbc.Row([
                    dbc.Col(dbc.Button("Actualizar contexto", id="mod3-btn-actualizar-contexto",
                                       color="primary", className="w-100"), lg=12)
                ])
            ])
        ], className="mb-3"),

        # Resumen del contexto (preview)
        dbc.Card([
            dbc.CardHeader("Resumen del contexto actual", className="bg-light py-2"),
            dbc.CardBody([html.Pre(id="mod3-contexto-preview", style={"whiteSpace": "pre-wrap"})])
        ], className="mb-3"),

        # Área de Chat
        dbc.Card([
            dbc.CardHeader("Chat", className="bg-light py-2"),
            dbc.CardBody([
                html.Div(id="mod3-chat-mensajes", style={
                    "height": "380px", "overflowY": "auto", "border": "1px solid #ddd",
                    "padding": "8px", "borderRadius": "6px"
                }),
                dbc.Input(id="mod3-chat-input", type="text", placeholder="Escribe tu pregunta…", className="mt-3"),
                dbc.Button("Enviar", id="mod3-chat-enviar", color="primary", className="mt-2"),
                html.Div(id="mod3-chat-estado", className="text-muted mt-2")
            ])
        ], className="mb-4"),

        # Stores
        dcc.Store(id="mod3-store-context-datos"),   # registros del contexto (DF -> records)
        dcc.Store(id="mod3-store-chat-hist")        # historial [{role: 'user'|'assistant', 'text': '...'}]
    ], fluid=True, className="py-4")


def register_callbacks(app):
    # 1) Inicializar opciones de filtros
    @app.callback(
        [
            Output("mod3-input-banco", "options"),
            Output("mod3-input-clasificacion", "options"),
            Output("mod3-input-proveedor", "options"),
            Output("mod3-input-mes", "options"),
        ],
        [Input("mod3-btn-actualizar-contexto", "n_clicks")],
        prevent_initial_call=False
    )
    def mod3_inicializar_opciones(n_clicks):
        bancos = query_bancos()
        clasificaciones = query_clasificaciones()
        proveedores = query_proveedores_clientes()
        meses = query_meses_balance()
        opciones_banco = [{"label": b[0], "value": b[0]} for b in bancos]
        opciones_clas = [{"label": c[0], "value": c[0]} for c in clasificaciones]
        opciones_prov = [{"label": p[0], "value": p[0]} for p in proveedores]
        opciones_mes = [{"label": m, "value": m} for m in meses]
        return opciones_banco, opciones_clas, opciones_prov, opciones_mes

    # 2) Actualizar el contexto (cargar DF del balance_general con filtros)
    @app.callback(
        [Output("mod3-store-context-datos", "data"),
         Output("mod3-contexto-preview", "children"),
         Output("mod3-chat-estado", "children")],
        [Input("mod3-btn-actualizar-contexto", "n_clicks")],
        [State("mod3-input-banco", "value"),
         State("mod3-input-clasificacion", "value"),
         State("mod3-input-proveedor", "value"),
         State("mod3-input-mes", "value")],
        prevent_initial_call=True
    )
    def mod3_actualizar_contexto(n_clicks, bancos_sel, clas_sel, prov_sel, mes_sel):
        try:
            data = query_mod1(bancos_sel, clas_sel, prov_sel, mes_sel)
            if not data:
                return {}, "No hay datos con los filtros seleccionados.", "Contexto actualizado (0 filas)."
            df = pd.DataFrame(data, columns=['id','fecha', 'banco', 'referencia', 'concepto',
                                             'clasificacion', 'proveedor_cliente', 'monto'])
            preview = construir_contexto_mod1(df)[:1200]  # recorta preview
            return df.to_dict("records"), preview, f"Contexto actualizado ({len(df)} filas)."
        except Exception as e:
            return {}, f"Error construyendo contexto: {str(e)}", "Error actualizando el contexto."


    # 3) Enviar mensaje al chatbot (usa contexto del store)
    @app.callback(
        [Output("mod3-chat-mensajes", "children"),
         Output("mod3-store-chat-hist", "data"),
         Output("mod3-chat-input", "value"),
         Output("mod3-chat-estado", "children", allow_duplicate=True)],
        [Input("mod3-chat-enviar", "n_clicks")],
        [State("mod3-chat-input", "value"),
         State("mod3-store-context-datos", "data"),
         State("mod3-store-chat-hist", "data")],
        prevent_initial_call=True
    )
    def mod3_enviar_mensaje(n_clicks, mensaje, contexto_data, hist):
        if not mensaje:
            return no_update, no_update, "", "Escribe un mensaje para continuar."

        # Construir DF desde el store (o vacío)
        df_ctx = pd.DataFrame(contexto_data) if contexto_data else pd.DataFrame()

        # Generar respuesta con chat.py (Gemini si disponible, sino fallback local)
        try:
            respuesta = responder_con_contexto(mensaje, df_ctx)
        except Exception as e:
            respuesta = f"Error generando respuesta: {str(e)}"

        # Actualizar historial
        hist = hist or []
        hist.append({"role": "user", "text": mensaje})
        hist.append({"role": "assistant", "text": respuesta})

        # Render simple del historial
        items = []
        for h in hist:
            if h["role"] == "user":
                items.append(html.Div([
                    html.Strong("Tú: "), html.Span(h["text"])
                ], style={"marginBottom": "6px"}))
            else:
                items.append(html.Div([
                    html.Strong("Asistente: "), html.Span(h["text"])
                ], style={"marginBottom": "10px", "whiteSpace": "pre-wrap"}))

        return items, hist, "", "Mensaje enviado."
