from dash import dcc, html
import dash_bootstrap_components as dbc
from .components import mod1_balances, mod2_saldos, mod3_chatbot, mod4_cuadre, mod5_resumen, mod6_detalle, mod7_cashflow



# Layout principal
def get_main_layout():
    return html.Div([
    dcc.Location(id='url', refresh=False),

    # Navbar 
    get_navbar(),

    # Contenido de la página (cambia según la navegación)
    html.Div(id='page-content', className='flex-grow-1'),


    # Footer (siempre visible)
    get_footer(),

], className='d-flex flex-column min-vh-100')

# Layout de página principal
main_layout = dbc.Container([
    
    html.H1("Reporte de flujo de caja", className="text-4xl font-bold text-gray-900 mb-4 text-center"),

    # Botones de navegación
    dbc.Row([
        dbc.Col([
            dbc.Button("Balances por clasificación y banco",
                       id="btn-modulo-1",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-1")
        ]),
        dbc.Col([
            dbc.Button("Disponibilidad bancaria",
                       id="btn-modulo-2",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-2")
        ]),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Chatbot",
                       id="btn-modulo-3",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-3")
        ]),
        dbc.Col([
            dbc.Button("Cuadre de Cuentas",
                       id="btn-modulo-4",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-4")
        ]),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Resumen mensual",
                       id="btn-modulo-5",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-5")
        ]),
        dbc.Col([
            dbc.Button("Resumen detallado",
                       id="btn-modulo-6",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-6")
        ]),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Flujo de caja",
                       id="btn-modulo-7",
                       color="primary",
                       className="w-100 mb-2",
                       href="/modulo-7")
        ]),
    ], className="mb-4"),
    # Contenido principal
    html.Div(id='main-content')
], fluid=True)


def get_navbar():
    return dbc.Navbar([
            dbc.Container([
                # Logo y marca
                dbc.NavbarBrand([
                
                    html.Span("Reporte de flujo de caja", className="ms-2 bold fs-4")
                ], href="/", className="fw-bold"),

                # Botón a la derecha con ml-auto (margen izquierdo automático)
                dbc.Button(
                    "Volver al Inicio",
                    id="btn-volver-inicio",
                    color="light",
                    href="/",
                    className="ms-auto",
                    outline=True
                ),
            ], fluid=True)
        ], color="primary", dark=True, className="mb-4")

def get_footer():
    return html.Footer(
        "© 2025 EY - Todos los derechos reservados.",
        className="text-center py-3 bg-primary text-white",
    )

# Layouts de módulos 
modulo_1_layout = mod1_balances.get_layout()
modulo_2_layout = mod2_saldos.get_layout()
modulo_3_layout = mod3_chatbot.get_layout()
modulo_4_layout = mod4_cuadre.get_layout()
modulo_5_layout = mod5_resumen.get_layout()
modulo_6_layout = mod6_detalle.get_layout()
modulo_7_layout = mod7_cashflow.get_layout()