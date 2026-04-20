from dash import Output, Input, State
from app.layouts import main_layout, modulo_1_layout, modulo_2_layout, modulo_3_layout, modulo_4_layout, modulo_5_layout, modulo_6_layout, modulo_7_layout
from app.components import mod1_balances, mod2_saldos, mod3_chatbot, mod4_cuadre, mod5_resumen, mod6_detalle, mod7_cashflow

def register_callbacks(app):
    # Callback principal para navegación
    @app.callback(Output('page-content', 'children'),
                [Input('url', 'pathname')])
    def display_page(pathname):
        match pathname:
            case '/modulo-1':
                return modulo_1_layout
            case '/modulo-2':
                return modulo_2_layout
            case '/modulo-3':
                return modulo_3_layout
            case '/modulo-4':
                return modulo_4_layout
            case '/modulo-5':
                return modulo_5_layout
            case '/modulo-6':
                return modulo_6_layout
            case '/modulo-7':
                return modulo_7_layout
            case _:
                return main_layout
    

    # Registrar callbacks de cada módulo
    mod1_balances.register_callbacks(app)
    mod2_saldos.register_callbacks(app)
    mod3_chatbot.register_callbacks(app)
    mod4_cuadre.register_callbacks(app)
    mod5_resumen.register_callbacks(app)
    mod6_detalle.register_callbacks(app)
    mod7_cashflow.register_callbacks(app)
    
    # Callback para controlar la visibilidad del botón "Volver al Inicio"
    @app.callback(
        Output("btn-volver-inicio", "style"),
        [Input("url", "pathname")]
    )
    def toggle_volver_button(pathname):
        # Mostrar el botón en todas las páginas excepto en la principal
        if pathname == "/" or pathname is None:
            return {"display": "none"}
        else:
            return {"display": "block"}
