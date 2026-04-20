from app.utils import _fmt_money
import pandas as pd
from dotenv import load_dotenv
import os
from openai import AzureOpenAI

# Cargar variables de entorno desde .env
load_dotenv()

# ============================================
# CONFIGURACIÓN DEL CLIENTE AZURE OPENAI
# ============================================
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://danie-mklcyg00-eastus2.cognitiveservices.azure.com/")
AZURE_API_KEY = os.getenv("OPENAI_API_KEY")
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-12-01-preview")
AZURE_MODEL_NAME = os.getenv("AZURE_MODEL_NAME", "gpt-5-nano")

# Inicializar cliente Azure OpenAI
try:
    azure_client = AzureOpenAI(
        api_version=AZURE_API_VERSION,
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
    )
    AZURE_AVAILABLE = bool(AZURE_API_KEY)
except Exception as e:
    print(f"Error configurando Azure OpenAI: {e}")
    azure_client = None
    AZURE_AVAILABLE = False


def llamar_ia_azure(mensaje: str, contexto: str) -> str:
    """
    Llama al modelo Azure OpenAI con un prompt adaptado al contexto del balance general (Mod1).
    El 'contexto' es un texto estructurado generado por 'construir_contexto_mod1(df)'.
    
    Args:
        mensaje: La pregunta del usuario
        contexto: Contexto textual del balance general
    
    Returns:
        La respuesta del modelo como string
    """
    try:
        if not AZURE_AVAILABLE or azure_client is None:
            return "Servicio de IA no disponible. Por favor, configura tu API key de Azure OpenAI."

        system_prompt = """Eres un analista financiero especializado en conciliación bancaria y análisis de transacciones.
Tu única fuente de verdad es el CONTEXTO que se te proporciona (derivado de la tabla 'balance_general' del Mod1).

ESQUEMA DE DATOS (Mod1):
- fecha (rango temporal)
- banco
- referencia (identificador de la transacción)
- concepto (descripción)
- clasificacion
- proveedor_cliente
- monto (USD)

INSTRUCCIONES:
1) Responde SOLO con base en el CONTEXTO. Si la información no está en el contexto, dilo claramente.
2) Sé breve, claro y accionable (máximo ~220 palabras). Usa viñetas cuando ayuden a la lectura.
3) Cuando hables de montos, usa formato de moneda como en el contexto (ej.: $ 12.345,67).
4) Si el usuario pide desglose, filtra y compara por: banco, clasificación y proveedor/cliente.
5) Prioriza insights útiles: top transacciones por monto, bancos con mayor/menor aporte, concentraciones por proveedor/cliente, movimientos inusuales (outliers), posibles duplicados por referencia y montos negativos.
6) Si faltan filtros (p. ej., periodo, banco, proveedor/cliente), pide una aclaración concreta.
7) No inventes datos, no asumas campos que no están en el contexto. No cites fuentes externas.
8) Si el usuario pregunta "cómo cambiar clasificación/proveedor", aclara que se puede editar en el Módulo 1 seleccionando la fila y usando los editores debajo de la tabla.

Responde en español. Estructura sugerida:
- Resumen corto (1–2 líneas)
- Hallazgos clave (viñetas)
- Siguientes pasos / recomendaciones (viñetas)"""

        user_content = f"""CONTEXTO:
{contexto}

PREGUNTA DEL USUARIO:
{mensaje}"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ]

        response = azure_client.chat.completions.create(
            messages=messages,
            max_completion_tokens=16384,
            model=AZURE_MODEL_NAME
        )

        respuesta_texto = response.choices[0].message.content
        
        if not respuesta_texto or not respuesta_texto.strip():
            return "No se pudo generar una respuesta en este momento. Intenta nuevamente.",
        
        return respuesta_texto.strip()

    except Exception as e:
        error_msg = f"Error conectando con Azure OpenAI: {str(e)}"
        print(f"[ERROR] {error_msg}")
        return f"Error al procesar tu mensaje: {str(e)}"


def get_chat_response(user_message: str, conversation_history: list = None, system_prompt: str = None) -> str:
    """
    Envía un mensaje al modelo y obtiene una respuesta (uso general).
    
    Args:
        user_message: El mensaje del usuario
        conversation_history: Lista opcional de mensajes previos para mantener contexto
                            Formato: [{"role": "user/assistant", "content": "mensaje"}, ...]
        system_prompt: Prompt del sistema personalizado (opcional)
    
    Returns:
        La respuesta del modelo como string
    """
    try:
        if not AZURE_AVAILABLE or azure_client is None:
            return "Servicio de IA no disponible. Por favor, configura tu API key de Azure OpenAI."
        
        default_system = "Eres un asistente útil que responde en español de manera clara y concisa."
        messages = [{"role": "system", "content": system_prompt or default_system}]
        
        if conversation_history:
            messages.extend(conversation_history)
        
        messages.append({"role": "user", "content": user_message})
        
        response = azure_client.chat.completions.create(
            messages=messages,
            max_completion_tokens=2048,
            model=AZURE_MODEL_NAME
        )
        
        return response.choices[0].message.content
    
    except Exception as e:
        error_msg = f"Error al comunicarse con el servicio: {str(e)}"
        print(f"[ERROR] {error_msg}")
        return f"Lo siento, hubo un error al procesar tu mensaje. Por favor, intenta de nuevo. ({str(e)})"


def analisis_local(mensaje, contexto, df_ctx: pd.DataFrame = None):
    """
    Análisis local orientado al contexto de 'balance_general' (Mod1).
    Si df_ctx está disponible, calcula KPIs y hallazgos útiles.
    """
    mensaje_l = (mensaje or "").lower()

    # Si no hay DF, devolvemos recomendaciones basadas solo en el contexto textual
    if df_ctx is None or df_ctx.empty:
        base = f"Resumen de datos (desde el contexto):\n{contexto}\n\n"
        return (
            base
            + "Sugerencias:\n"
            "- Pide un periodo concreto o filtra por Banco/Clasificación/Proveedor para un análisis más fino.\n"
            "- Solicita 'top transacciones por monto' o 'posibles duplicados por referencia'.\n"
            "- Si necesitas editar clasificación/proveedor, hazlo en el Módulo 1 (editor bajo la tabla)."
        )

    # --- Cálculos principales con DF ---
    df = df_ctx.copy()
    # Tipos robustos
    df["monto"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)
    fechas = pd.to_datetime(df["fecha"], errors="coerce")
    fmin, fmax = fechas.min(), fechas.max()

    total_reg = len(df)
    total_monto = df["monto"].sum()
    prom_tx = total_monto / total_reg if total_reg else 0

    # Tops
    top_bancos = df.groupby("banco")["monto"].sum().sort_values(ascending=False).head(3)
    top_clas   = df.groupby("clasificacion")["monto"].sum().sort_values(ascending=False).head(3)
    top_prov   = df.groupby("proveedor_cliente")["monto"].sum().sort_values(ascending=False).head(3)
    top_refs   = df.groupby(["referencia", "concepto"])["monto"].sum().sort_values(ascending=False).head(3)

    # Duplicados por referencia
    dups = df["referencia"].value_counts()
    duplicadas = dups[dups > 1].head(3)

    # Negativas
    neg_count = (df["monto"] < 0).sum()

    # Outliers básicos (percentil 95)
    p95 = df["monto"].quantile(0.95) if total_reg > 0 else 0
    outs = df[df["monto"] >= p95].sort_values("monto", ascending=False).head(3)

    # Build respuesta según intención simple
    secciones = []

    # Resumen
    rango_txt = f"{fmin.date()} a {fmax.date()}" if pd.notnull(fmin) and pd.notnull(fmax) else "rango no disponible"
    secciones.append(
        f"**Resumen ({rango_txt})**\n"
        f"- Registros: {total_reg:,}".replace(",", ".")
        + f"\n- Monto total: {_fmt_money(total_monto)}"
        + f"\n- Promedio por transacción: {_fmt_money(prom_tx)}"
    )

    # Hallazgos clave
    def _serie_to_lines(serie, titulo):
        if serie.empty:
            return f"- {titulo}: sin datos."
        lines = [f"- {titulo}:"]
        for k, v in serie.items():
            lines.append(f"  • {k}: {_fmt_money(v)}")
        return "\n".join(lines)

    secciones.append(_serie_to_lines(top_bancos, "TOP bancos por monto"))
    secciones.append(_serie_to_lines(top_clas, "TOP clasificaciones por monto"))
    secciones.append(_serie_to_lines(top_prov, "TOP proveedor/cliente por monto"))

    # Top referencias
    if not top_refs.empty:
        lines = ["- TOP referencias por monto:"]
        for (ref, conc), val in top_refs.items():
            conc_txt = (conc or "")[:80]
            lines.append(f"  • {ref} — {conc_txt}: {_fmt_money(val)}")
        secciones.append("\n".join(lines))
    else:
        secciones.append("- TOP referencias por monto: sin datos.")

    # Outliers
    if not outs.empty and p95 > 0:
        lines = [f"- Posibles outliers (≥ P95={_fmt_money(p95)}):"]
        for _, r in outs.iterrows():
            lines.append(f"  • {r.get('referencia','')} — {r.get('concepto','')[:80]}: {_fmt_money(r['monto'])}")
        secciones.append("\n".join(lines))

    # Duplicados y negativas
    if not duplicadas.empty:
        lines = ["- Posibles duplicados por referencia:"]
        for ref, cnt in duplicadas.items():
            lines.append(f"  • {ref}: {cnt} ocurrencias")
        secciones.append("\n".join(lines))
    if neg_count > 0:
        secciones.append(f"- Transacciones negativas: {neg_count}")

    # Recomendaciones
    recs = ["**Recomendaciones**"]
    if "duplic" in mensaje_l:
        recs.append("- Revisar referencias con más de una ocurrencia; conciliar contra extractos.")
    if "outlier" in mensaje_l or "grande" in mensaje_l or "alto" in mensaje_l:
        recs.append("- Validar transacciones por encima de P95; confirmar soporte y aprobación.")
    if "clasific" in mensaje_l:
        recs.append("- Si alguna transacción está mal clasificada, edítala en el Módulo 1 (editor bajo la tabla).")
    if "proveedor" in mensaje_l or "cliente" in mensaje_l:
        recs.append("- Para corregir proveedor/cliente, usa el editor del Módulo 1.")
    if len(recs) == 1:
        recs.extend([
            "- Si necesitas foco: indica banco, clasificación o proveedor/cliente específicos.",
            "- Solicita un periodo concreto para análisis temporal (semana/mes/trimestre)."
        ])
    secciones.append("\n".join(recs))

    return "\n\n".join(secciones)




def construir_contexto_mod1(df: pd.DataFrame) -> str:
    """
    Construye un contexto textual breve y estructurado a partir de balance_general:
    - totales (registros/monto/promedio),
    - top bancos / clasificaciones / proveedor_cliente,
    - top referencias por monto,
    - rango de fechas.
    """
    if df is None or df.empty:
        return "No hay datos disponibles para el contexto."

    total_registros = len(df)
    total_monto = df["monto"].sum()
    promedio = (total_monto / total_registros) if total_registros > 0 else 0

    # Agrupados
    top_bancos = df.groupby("banco")["monto"].sum().sort_values(ascending=False).head(5)
    top_clas = df.groupby("clasificacion")["monto"].sum().sort_values(ascending=False).head(5)
    top_prov = df.groupby("proveedor_cliente")["monto"].sum().sort_values(ascending=False).head(5)
    top_refs = df.groupby(["referencia", "concepto"])["monto"].sum().sort_values(ascending=False).head(5)

    # Fechas (si 'fecha' es datetime o string, lo renderizamos robusto)
    try:
        fechas = pd.to_datetime(df["fecha"], errors="coerce")
        fmin = fechas.min()
        fmax = fechas.max()
        rango = f"Rango de fechas: {fmin.date()} a {fmax.date()}" if pd.notnull(fmin) and pd.notnull(fmax) else ""
    except Exception:
        rango = ""

    ctx = []
    ctx.append("RESUMEN DEL BALANCE GENERAL (contexto del chat)")
    ctx.append(f"- Total de registros: {total_registros:,}".replace(",", "."))
    ctx.append(f"- Monto total: {_fmt_money(total_monto)}")
    ctx.append(f"- Promedio por transacción: {_fmt_money(promedio)}")
    if rango: ctx.append(f"- {rango}")

    def _list_to_lines(serie, titulo):
        lines = [f"{titulo}:"]
        for idx, val in serie.items():
            label = idx if isinstance(idx, str) else str(idx)
            lines.append(f"  • {label}: {_fmt_money(val)}")
        return "\n".join(lines)

    ctx.append(_list_to_lines(top_bancos, "TOP 5 Bancos por monto"))
    ctx.append(_list_to_lines(top_clas, "TOP 5 Clasificaciones por monto"))
    ctx.append(_list_to_lines(top_prov, "TOP 5 Proveedor/Cliente por monto"))

    # Top referencias con concepto
    lines_refs = ["TOP 5 Referencias por monto:"]
    for (ref, conc), val in top_refs.items():
        ref_label = str(ref)
        conc_label = (conc or "")[:80]
        lines_refs.append(f"  • {ref_label} — {conc_label}: {_fmt_money(val)}")
    ctx.append("\n".join(lines_refs))

    # Nota final para IA
    ctx.append("NOTA: Responde en base a estos datos. Evita suposiciones fuera del contexto.")
    return "\n".join(ctx)


def responder_con_contexto(mensaje: str, df_ctx: pd.DataFrame) -> str:
    """
    Construye el contexto desde DF y llama a Azure OpenAI si está disponible,
    si falla o no hay servicio, usa 'analisis_local' con el DF.
    """
    contexto = construir_contexto_mod1(df_ctx)
    try:
        respuesta = llamar_ia_azure(mensaje, contexto)
        # Si Azure OpenAI devolvió un mensaje de no disponibilidad o error, hacemos fallback:
        if isinstance(respuesta, str) and ("no disponible" in respuesta.lower() or "error" in respuesta.lower()):
            return analisis_local(mensaje, contexto, df_ctx=df_ctx)
        return respuesta
    except Exception:
        return analisis_local(mensaje, contexto, df_ctx=df_ctx)

