
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# ========== CONFIGURACION DE LA BASE DE DATOS ==========
load_dotenv()
db_config = {
    'host': os.getenv("host"),
    'port': os.getenv("port"),
    'database': os.getenv("database"),
    'user': os.getenv("user"),
    'password': os.getenv("password"),
}
connection_str = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_str)
Session = sessionmaker(bind=engine)

# ========== QUERIES Generales ==========
def query_bancos():
    session = Session()
    query = text("""
        SELECT DISTINCT banco
        FROM balance_general
    """)
    bancos = session.execute(query).fetchall()
    session.close()
    return bancos


def query_clasificaciones():
    session = Session()
    query = text("""
        SELECT DISTINCT clasificacion
        FROM balance_general
        WHERE clasificacion IS NOT NULL AND clasificacion <> ''
        ORDER BY clasificacion
    """)
    clasificaciones = session.execute(query).fetchall()
    session.close()
    return clasificaciones


def query_proveedores_clientes():
    session = Session()
    query = text("""
        SELECT DISTINCT proveedor_cliente
        FROM balance_general
        WHERE proveedor_cliente IS NOT NULL AND proveedor_cliente <> ''
        ORDER BY proveedor_cliente
    """)
    proveedores = session.execute(query).fetchall()
    session.close()
    return proveedores


def query_meses_balance():
    """
    Devuelve los meses disponibles en la tabla balance_general
    en formato 'YYYY-MM', ordenados ascendentemente.
    """
    session = Session()
    query = text("""
        SELECT DISTINCT to_char(fecha, 'YYYY-MM') AS mes
        FROM balance_general
        ORDER BY mes
    """)
    data = session.execute(query).fetchall()
    session.close()
    # Devolvemos una lista simple ['2025-01', '2025-02', ...]
    return [row[0] for row in data]


# ========== QUERIES Modulo 1 ==========

def query_mod1(bancos, clasificaciones, proveedores_clientes, mes=None):
    session = Session()
    # Selecciona también proveedor_cliente
    query = """
        SELECT id,fecha, banco, referencia_bancaria,moneda_ref, descripcion, clasificacion, proveedor_cliente, monto_usd
        FROM balance_general
        WHERE 1=1
    """
    params = {}
    if bancos and len(bancos) > 0:
        query += " AND banco IN :bancos"
        params['bancos'] = tuple(bancos)
    if clasificaciones and len(clasificaciones) > 0:
        query += " AND clasificacion IN :clasificaciones"
        params['clasificaciones'] = tuple(clasificaciones)
    if proveedores_clientes and len(proveedores_clientes) > 0:
        query += " AND proveedor_cliente IN :proveedores"
        params['proveedores'] = tuple(proveedores_clientes)
    if mes:
        # Filtrar por mes en formato 'YYYY-MM' comparando contra la fecha
        query += " AND to_char(fecha, 'YYYY-MM') = :mes"
        params['mes'] = mes

    data = session.execute(text(query), params).fetchall()
    session.close()
    return data


# ========== UPDATE de Clasificación ==========
def update_clasificacion(id: str, nueva_clasificacion: str) -> int:
    session = Session()
    try:
        query = text("""
            UPDATE balance_general
            SET clasificacion = :clasificacion
            WHERE id = :id
        """)
        result = session.execute(query, {
            "clasificacion": nueva_clasificacion,
            "id": id
        })
        session.commit()
        return result.rowcount or 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# ========== UPDATE de Proveedor/Cliente ==========
def update_proveedor_cliente(id: str, nuevo_proveedor: str) -> int:
    session = Session()
    try:
        query = text("""
            UPDATE balance_general
            SET proveedor_cliente = :proveedor
            WHERE id = :id
        """)
        result = session.execute(query, {
            "proveedor": nuevo_proveedor,
            "id": id
        })
        session.commit()
        return result.rowcount or 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# ========== QUERIES Modulo 2 ==========

def query_meses_disponibles():
    """Obtiene los meses disponibles en resumen_saldos en formato YYYY-MM."""
    session = Session()
    query = text("""
        SELECT DISTINCT ano || '-' || LPAD(mes::text, 2, '0') AS mes, ano, mes AS mes_num
        FROM resumen_saldos
        ORDER BY ano, mes_num
    """)
    data = session.execute(query).fetchall()
    session.close()
    return [row[0] for row in data]


def query_semanas_disponibles(mes: str = None):
    """Obtiene las semanas disponibles, opcionalmente filtradas por mes en formato YYYY-MM."""
    session = Session()
    if mes:
        ano, mes_num = mes.split('-')
        query = text("""
            SELECT DISTINCT semana
            FROM resumen_saldos
            WHERE ano = :ano AND mes = :mes_num
            ORDER BY semana
        """)
        data = session.execute(query, {"ano": int(ano), "mes_num": int(mes_num)}).fetchall()
    else:
        query = text("""
            SELECT DISTINCT semana
            FROM resumen_saldos
            ORDER BY semana
        """)
        data = session.execute(query).fetchall()
    session.close()
    return [row[0] for row in data]


def query_saldos_finales(moneda_ref: str, mes: str = None, semana: int = None):
    """Obtiene saldos finales filtrados por moneda, mes y semana."""
    session = Session()
    
    # Si no se especifica mes/semana, usar los máximos
    if mes and semana:
        ano, mes_num = mes.split('-')
        query = text("""
            SELECT banco, saldo_usd AS Saldo_Final, ano || '-' || LPAD(mes::text, 2, '0') AS fecha, semana, moneda_ref
            FROM resumen_saldos
            WHERE moneda_ref = :moneda_ref
            AND ano = :ano
            AND mes = :mes_num
            AND semana = :semana
            ORDER BY banco
        """)
        params = {"moneda_ref": moneda_ref, "ano": int(ano), "mes_num": int(mes_num), "semana": semana}
    else:
        query = text("""
            SELECT banco, saldo_usd AS Saldo_Final, ano || '-' || LPAD(mes::text, 2, '0') AS fecha, semana, moneda_ref
            FROM resumen_saldos rs
            WHERE moneda_ref = :moneda_ref
            AND semana = (
                SELECT max(semana)
                FROM resumen_saldos
                WHERE moneda_ref = :moneda_ref
                AND rs.banco = banco
            )
            ORDER BY banco, ano, semana
        """)
        params = {"moneda_ref": moneda_ref}
    
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_saldos_iniciales_semana(moneda_ref: str, mes: str = None, semana: int = None):
    """Obtiene saldos iniciales de la semana desde resumen_saldos."""
    session = Session()
    
    if mes and semana:
        ano, mes_num = mes.split('-')
        query = text("""
            SELECT banco, saldo_usd AS Saldo_Inicial, ano || '-' || LPAD(mes::text, 2, '0') AS fecha, semana
            FROM saldos_iniciales
            WHERE moneda_ref = :moneda_ref
            AND ano = :ano
            AND mes = :mes_num
            AND semana = :semana
            ORDER BY banco
        """)
        params = {"moneda_ref": moneda_ref, "ano": int(ano), "mes_num": int(mes_num), "semana": semana}
    else:
        query = text("""
            SELECT banco, saldo_usd AS Saldo_Inicial, ano || '-' || LPAD(mes::text, 2, '0') AS fecha, semana
            FROM resumen_saldos rs
            WHERE moneda_ref = :moneda_ref
            AND semana = (
                SELECT max(semana)
                FROM saldos_iniciales
                WHERE moneda_ref = :moneda_ref
                AND rs.banco = banco
            )
            ORDER BY banco
        """)
        params = {"moneda_ref": moneda_ref}
    
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_saldos_iniciales(moneda_ref: str):
    """Obtiene saldos iniciales del mes desde la tabla saldos_iniciales."""
    session = Session()
    query = text("""
        SELECT banco, saldo_usd AS Saldo_Inicial, mes as fecha
        FROM saldos_iniciales
        WHERE moneda_ref = :moneda_ref
    """)
    params = {"moneda_ref": moneda_ref}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_masa_monetaria(moneda_ref: str, mes: str = None, semana: int = None):
    """Calcula la masa monetaria total filtrada por moneda, mes y semana."""
    session = Session()
    
    if mes and semana:
        ano, mes_num = mes.split('-')
        query = text("""
            SELECT SUM(saldo_usd)
            FROM resumen_saldos
            WHERE moneda_ref = :moneda_ref
            AND ano = :ano
            AND mes = :mes_num
            AND semana = :semana
        """)
        params = {"moneda_ref": moneda_ref, "ano": int(ano), "mes_num": int(mes_num), "semana": semana}
    else:
        query = text("""
            SELECT SUM(saldo_usd)
            FROM resumen_saldos rs
            WHERE moneda_ref = :moneda_ref
            AND semana = (
                SELECT max(semana)
                FROM resumen_saldos
                WHERE moneda_ref = :moneda_ref
                AND rs.banco = banco
            )
        """)
        params = {"moneda_ref": moneda_ref}
    
    masa = session.execute(query, params).scalar()
    session.close()
    return masa


# ========== QUERIES Modulo 4 ==========

def query_saldo_inicial_banco_mes(banco: str, mes: str):
    """
    Obtiene el saldo inicial de un banco en un mes específico (semana 1 del mes).
    Suma todos los saldos (todas las monedas y referencias) del banco.
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT COALESCE(SUM(saldo_usd), 0) AS Saldo_Inicial
        FROM saldos_iniciales
        WHERE banco = :banco
        AND mes = :mes_num
        AND semana = 1
    """)
    params = {"banco": banco, "mes_num": mes_num}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_saldo_final_banco_mes(banco: str, mes: str):
    """
    Obtiene el saldo final de un banco en un mes específico (última semana del mes).
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT COALESCE(SUM(saldo_usd), 0) AS Saldo_Final
        FROM resumen_saldos
        WHERE banco = :banco
        AND mes = :mes_num
        AND semana = (
            SELECT MAX(semana)
            FROM resumen_saldos
            WHERE banco = :banco
            AND mes = :mes_num
        )
    """)
    params = {"banco": banco, "mes_num": mes_num}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_suma_movimientos_banco_mes(banco: str, mes: str):
    """
    Obtiene la suma algebraica de movimientos (monto_usd) de un banco en un mes específico.
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    query = text("""
        SELECT COALESCE(SUM(monto_usd), 0) AS suma_movimientos
        FROM balance_general
        WHERE banco = :banco
        AND to_char(fecha, 'YYYY-MM') = :mes
    """)
    params = {"banco": banco, "mes": mes}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_moneda_ref_banco_mes(banco: str, mes: str):
    """
    Obtiene la moneda_ref principal de un banco en un mes específico.
    Si hay múltiples monedas, devuelve la más común o 'BS' por defecto.
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT moneda_ref
        FROM resumen_saldos
        WHERE banco = :banco
        AND mes = :mes_num
        GROUP BY moneda_ref
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """)
    params = {"banco": banco, "mes_num": mes_num}
    result = session.execute(query, params).scalar()
    session.close()
    # Si no encuentra en resumen_saldos, buscar en balance_general
    if result is None:
        query = text("""
            SELECT moneda_ref
            FROM balance_general
            WHERE banco = :banco
            AND to_char(fecha, 'YYYY-MM') = :mes
            GROUP BY moneda_ref
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """)
        params = {"banco": banco, "mes": mes}
        result = session.execute(query, params).scalar()
    return result if result is not None else 'BS'


# ========== QUERIES Modulo 5 ==========

def query_ingresos_mes(mes: str):
    """
    Obtiene el total de ingresos (CREDITO) del mes de todos los bancos.
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    query = text("""
        SELECT COALESCE(SUM(monto_usd), 0) AS total_ingresos
        FROM balance_general
        WHERE tipo_operacion = 'CREDITO'
        AND to_char(fecha, 'YYYY-MM') = :mes
    """)
    params = {"mes": mes}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_egresos_mes(mes: str):
    """
    Obtiene el total de egresos (DEBITO) del mes de todos los bancos.
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    query = text("""
        SELECT COALESCE(SUM(monto_usd), 0) AS total_egresos
        FROM balance_general
        WHERE tipo_operacion = 'DEBITO'
        AND to_char(fecha, 'YYYY-MM') = :mes
    """)
    params = {"mes": mes}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_saldo_inicial_total_mes(mes: str):
    """
    Obtiene el saldo inicial total del mes (suma de todos los bancos, semana 1).
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT COALESCE(SUM(saldo_usd), 0) AS saldo_inicial_total
        FROM saldos_iniciales
        WHERE mes = :mes_num
        AND semana = 1
    """)
    params = {"mes_num": mes_num}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_saldo_final_total_mes(mes: str):
    """
    Obtiene el saldo final total del mes (suma de todos los bancos, última semana del mes).
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT COALESCE(SUM(saldo_usd), 0) AS saldo_final_total
        FROM resumen_saldos rs
        WHERE mes = :mes_num
        AND semana = (
            SELECT MAX(semana)
            FROM resumen_saldos
            WHERE mes = :mes_num
            AND banco = rs.banco
        )
    """)
    params = {"mes_num": mes_num}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


# ========== QUERIES Módulo 6: Ingresos / Egresos / Flujo de Caja ==========

def query_ingresos_agrupados(bancos, mes: str = None):
    """
    Obtiene los ingresos (SUM(monto_usd) > 0) agrupados por clasificación,
    filtrando opcionalmente por bancos y por mes (formato 'YYYY-MM').
    """
    session = Session()
    query = """
        SELECT clasificacion, SUM(monto_usd) AS importe
        FROM balance_general
        WHERE 1=1
    """
    params = {}

    if bancos and len(bancos) > 0:
        query += " AND banco IN :bancos"
        params["bancos"] = tuple(bancos)

    if mes:
        query += " AND to_char(fecha, 'YYYY-MM') = :mes"
        params["mes"] = mes

    query += """
        GROUP BY clasificacion
        HAVING SUM(monto_usd) > 0
        ORDER BY importe DESC
    """

    data = session.execute(text(query), params).fetchall()
    session.close()
    return data


def query_egresos_agrupados(bancos, mes: str = None):
    """
    Obtiene los egresos (SUM(monto_usd) < 0) agrupados por clasificación,
    filtrando opcionalmente por bancos y por mes (formato 'YYYY-MM').
    """
    session = Session()
    query = """
        SELECT clasificacion, SUM(monto_usd) AS importe
        FROM balance_general
        WHERE 1=1
    """
    params = {}

    if bancos and len(bancos) > 0:
        query += " AND banco IN :bancos"
        params["bancos"] = tuple(bancos)

    if mes:
        query += " AND to_char(fecha, 'YYYY-MM') = :mes"
        params["mes"] = mes

    query += """
        GROUP BY clasificacion
        HAVING SUM(monto_usd) < 0
        ORDER BY importe
    """

    data = session.execute(text(query), params).fetchall()
    session.close()
    return data


def query_flujo_caja_agrupado(bancos, mes: str = None):
    """
    Obtiene el flujo de caja agrupado por tipo_de_op (concepto general),
    uniendo contra la tabla detalle_cla.

    Devuelve:
      - concepto_gen (COALESCE(d.tipo_de_op, '05. Otros'))
      - importe (SUM(monto_usd))
    """
    session = Session()
    query = """
        SELECT
            COALESCE(d.tipo_de_op, '05. Otros (POR CLASIFICAR)') AS concepto_gen,
            SUM(bg.monto_usd) AS importe
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE 1=1
    """
    params = {}

    if bancos and len(bancos) > 0:
        query += " AND bg.banco IN :bancos"
        params["bancos"] = tuple(bancos)

    if mes:
        query += " AND to_char(bg.fecha, 'YYYY-MM') = :mes"
        params["mes"] = mes

    query += """
        GROUP BY concepto_gen
        ORDER BY concepto_gen
    """

    data = session.execute(text(query), params).fetchall()
    session.close()
    return data

# ========== QUERIES Módulo 7: Flujo de Caja ==========

def query_cobranzas_total_mes(mes: str):
    """
    Obtiene el total de cobranzas del mes (clasificación = 'Cobranzas').
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    query = text("""
        SELECT COALESCE(SUM(monto_usd), 0) AS total_cobranzas
        FROM balance_general
        WHERE clasificacion = 'Cobranzas'
        AND to_char(fecha, 'YYYY-MM') = :mes
    """)
    params = {"mes": mes}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_cobranzas_por_banco(mes: str):
    """
    Obtiene las cobranzas agrupadas por banco para un mes dado.
    """
    session = Session()
    query = text("""
        SELECT
            banco AS banco,
            COALESCE(SUM(monto_usd), 0) AS importe
        FROM balance_general
        WHERE clasificacion = 'Cobranzas'
        AND to_char(fecha, 'YYYY-MM') = :mes
        GROUP BY banco
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_cobranzas_por_moneda(mes: str):
    """
    Obtiene las cobranzas agrupadas por moneda de referencia para un mes dado.
    """
    session = Session()
    query = text("""
        SELECT
            moneda_ref AS moneda,
            COALESCE(SUM(monto_usd), 0) AS importe
        FROM balance_general
        WHERE clasificacion = 'Cobranzas'
        AND to_char(fecha, 'YYYY-MM') = :mes
        GROUP BY moneda_ref
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_cobranzas_por_cliente(mes: str):
    """
    Obtiene las cobranzas agrupadas por cliente/proveedor para un mes dado.
    Clientes nulos se agrupan bajo 'Otro'.
    """
    session = Session()
    query = text("""
        SELECT
            COALESCE(proveedor_cliente, 'Otro') AS cliente,
            COALESCE(SUM(monto_usd), 0) AS importe
        FROM balance_general
        WHERE clasificacion = 'Cobranzas'
        AND to_char(fecha, 'YYYY-MM') = :mes
        GROUP BY COALESCE(proveedor_cliente, 'Otro')
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_cobranzas_por_semana(mes: str):
    """
    Obtiene la evolución semanal de cobranzas dentro de un mes dado.
    Semana del mes calculada como LEAST(4, CEIL(día/7)).
    """
    session = Session()
    query = text("""
        SELECT
            LEAST(4, CEIL(EXTRACT(DAY FROM fecha)::numeric / 7)) AS semana_mes,
            COALESCE(SUM(monto_usd), 0) AS importe
        FROM balance_general
        WHERE clasificacion = 'Cobranzas'
        AND to_char(fecha, 'YYYY-MM') = :mes
        GROUP BY semana_mes
        ORDER BY semana_mes
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_pagos_operativos_total_mes(mes: str):
    """
    Obtiene el total de pagos operativos del mes
    (tipo_de_op = '02. EGRESOS OPERATIVOS').
    """
    session = Session()
    query = text("""
        SELECT
            ABS(COALESCE(SUM(bg.monto_usd), 0)) AS pago_op_total
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE d.tipo_de_op = '02. EGRESOS OPERATIVOS'
        AND to_char(bg.fecha, 'YYYY-MM') = :mes
    """)
    params = {"mes": mes}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_pagos_operativos_por_semana(mes: str):
    """
    Obtiene la evolución semanal de pagos operativos dentro de un mes dado.
    Semana del mes calculada como LEAST(4, CEIL(día/7)).
    """
    session = Session()
    query = text("""
        SELECT
            LEAST(4, CEIL(EXTRACT(DAY FROM bg.fecha)::numeric / 7)) AS semana_mes,
            ABS(COALESCE(SUM(bg.monto_usd), 0)) AS importe
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE d.tipo_de_op = '02. EGRESOS OPERATIVOS'
        AND to_char(bg.fecha, 'YYYY-MM') = :mes
        GROUP BY semana_mes
        ORDER BY semana_mes
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_masa_monetaria_mes(moneda_ref: str, mes: str):
    """
    Obtiene la masa monetaria (suma de saldo_usd) de una moneda específica
    al final del mes (última semana del mes) usando la tabla resumen_saldos.

    moneda_ref: 'BS' o 'US'
    mes: formato 'YYYY-MM' (ej: '2025-10')
    """
    session = Session()
    # Convertir 'YYYY-MM' a número de mes (extraer el mes)
    mes_num = int(mes.split('-')[1]) if '-' in mes else int(mes)
    query = text("""
        SELECT COALESCE(SUM(saldo_usd), 0) AS masa_monetaria
        FROM resumen_saldos rs
        WHERE mes = :mes_num
        AND moneda_ref = :moneda_ref
        AND semana = (
            SELECT MAX(semana)
            FROM resumen_saldos
            WHERE mes = :mes_num
              AND banco = rs.banco
              AND moneda_ref = :moneda_ref
        )
    """)
    params = {"mes_num": mes_num, "moneda_ref": moneda_ref}
    result = session.execute(query, params).scalar()
    session.close()
    return result if result is not None else 0.0


def query_pagos_operativos_por_banco(mes: str):
    """
    Obtiene los pagos operativos agrupados por banco para un mes dado.
    Usa tipo_de_op = '02. EGRESOS OPERATIVOS'.
    """
    session = Session()
    query = text("""
        SELECT
            bg.banco AS banco,
            ABS(COALESCE(SUM(bg.monto_usd), 0)) AS importe
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE d.tipo_de_op = '02. EGRESOS OPERATIVOS'
        AND to_char(bg.fecha, 'YYYY-MM') = :mes
        GROUP BY bg.banco
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_pagos_operativos_por_cliente(mes: str):
    """
    Obtiene los pagos operativos agrupados por proveedor/cliente para un mes dado.
    Usa tipo_de_op = '02. EGRESOS OPERATIVOS'.
    """
    session = Session()
    query = text("""
        SELECT
            COALESCE(bg.proveedor_cliente, 'Otro') AS cliente,
            ABS(COALESCE(SUM(bg.monto_usd), 0)) AS importe
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE d.tipo_de_op = '02. EGRESOS OPERATIVOS'
        AND to_char(bg.fecha, 'YYYY-MM') = :mes
        GROUP BY COALESCE(bg.proveedor_cliente, 'Otro')
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data


def query_pagos_operativos_por_moneda(mes: str):
    """
    Obtiene los pagos operativos agrupados por moneda_ref para un mes dado.
    Usa tipo_de_op = '02. EGRESOS OPERATIVOS'.
    """
    session = Session()
    query = text("""
        SELECT
            bg.moneda_ref AS moneda,
            ABS(COALESCE(SUM(bg.monto_usd), 0)) AS importe
        FROM balance_general bg
        LEFT JOIN detalle_cla d
            ON bg.clasificacion = d.concepto_ey
        WHERE d.tipo_de_op = '02. EGRESOS OPERATIVOS'
        AND to_char(bg.fecha, 'YYYY-MM') = :mes
        GROUP BY bg.moneda_ref
        ORDER BY importe DESC
    """)
    params = {"mes": mes}
    data = session.execute(query, params).fetchall()
    session.close()
    return data
