from dash import dash_table

MESES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}
MESES_INV = {v: k.capitalize() for k, v in MESES.items()}


def crear_tabla_estilizada(id, columns, data, page_size=100, estilo_adicional=None):
    """
    Crea una tabla Dash con estilos consistentes para toda la aplicación
    
    Args:
        id (str): ID de la tabla
        columns (list): Lista de diccionarios con definición de columnas
        data (list): Lista de diccionarios con los datos
        page_size (int): Número de filas por página
        estilo_adicional (list): Estilos condicionales adicionales
    
    Returns:
        dash_table.DataTable: Tabla estilizada
    """
    # Estilos base consistentes
    estilo_base = {
        'style_cell': {
            'textAlign': 'center',
            'padding': '4px',
            'minWidth': '60px',
            'width': 'auto',
            'maxWidth': '120px',
            'whiteSpace': 'normal',
            'fontSize': '12px',
            'fontFamily': 'Arial, sans-serif'
        },
        
        'style_header': {
            'backgroundColor': 'rgb(44, 62, 80)',
            'color': 'white',
            'fontWeight': 'bold',
            'padding': '6px',
            'textAlign': 'center',
            'fontSize': '13px'
        },
        'style_data': {
            'backgroundColor': 'rgb(248, 248, 248)',
            'color': 'black'
        },
        'style_table': {
            'overflowX': 'auto',
            'minWidth': '100%',
            'maxWidth': '100%',
            'margin': '0 auto',
            'height': '300px', 
            'overflowY': 'auto'
        }
    }
    
    # Estilos condicionales base
    estilo_condicional_base = [
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(240, 240, 240)'
        }
    ]
    
    # Combinar con estilos adicionales si se proporcionan
    if estilo_adicional:
        estilo_condicional_base.extend(estilo_adicional)
    
    return dash_table.DataTable(
        id=id,
        columns=columns,
        data=data,
        page_size=page_size,
        **estilo_base,
        style_data_conditional=estilo_condicional_base
    )


def crear_tabla_estilizada_select(id, columns, data, page_size=10, estilo_adicional=None):
    """
    Igual que crear_tabla_estilizada, pero habilita selección de filas (single)
    para usar en el Módulo 1.
    """
    estilo_base = {
        'style_cell': {
            'textAlign': 'center',
            'padding': '4px',
            'minWidth': '60px',
            'width': 'auto',
            'maxWidth': '120px',
            'whiteSpace': 'normal',
            'height': 'auto',
            'fontSize': '12px',
            'fontFamily': 'Arial, sans-serif'
        },
        'style_header': {
            'backgroundColor': 'rgb(44, 62, 80)',
            'color': 'white',
            'fontWeight': 'bold',
            'padding': '6px',
            'textAlign': 'center',
            'fontSize': '13px'
        },
        'style_data': {
            'backgroundColor': 'rgb(248, 248, 248)',
            'color': 'black'
        },
        'style_table': {
            'overflowX': 'auto',
            'minWidth': '100%',
            'maxWidth': '100%',
            'margin': '0 auto'
        }
    }
    estilo_condicional_base = [
        {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(240, 240, 240)'}
    ]
    if estilo_adicional:
        estilo_condicional_base.extend(estilo_adicional)

    return dash_table.DataTable(
        id=id,
        columns=columns,
        data=data,
        page_size=page_size,
        row_selectable="single",
        selected_rows=[],
        **estilo_base,
        style_data_conditional=estilo_condicional_base
    )


def _fmt_money(x, signo="$"):
    """Formatea un número como moneda con el símbolo $ al final"""
    try:
        if signo == "US":
            signo = "$"
        if signo == "BS":
            signo = "$"
        amount = float(x)
        if amount == 0: return f"{signo} 0,00"
        
        parts = f"{amount:,.2f}".split(".")
        integer_part = parts[0].replace(",", ".")
        decimal_part = parts[1] if len(parts) > 1 else "00"
        
        return f"{signo} {integer_part},{decimal_part}"
    except (ValueError, TypeError):
        return f"{signo} 0,00"