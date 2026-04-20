import dash
from dash import dcc, html
from app import layouts, callbacks
import dash_bootstrap_components as dbc


# Inicializar la aplicación Dash
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.FLATLY],
                suppress_callback_exceptions=True)



app.title = "EY Cashflow"


# Configurar layout básico que incluye navbar y footer
app.layout = layouts.get_main_layout()

# Registrar callbacks
callbacks.register_callbacks(app)

if __name__ == '__main__':
    app.run(debug=True, port=8090)