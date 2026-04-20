import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

# -----------------------------------------------------------------------------
# 📦 IMPORTS COMPARTIDOS
# -----------------------------------------------------------------------------
from scanner_archivos import ScannerAutomatico
from utils_comunes import obtener_semana_corte_viernes, GestorTasas

class NormalizadorBVC:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()

    def crear_carpetas(self):
        """
        Crea la carpeta de salida específica para BVC.
        """
        os.makedirs(f"{self.carpeta_salida}/bvc", exist_ok=True)

    def limpiar_numero(self, valor):
        """
        Limpieza robusta para formato Venezuela (1.000,00) -> Float.
        A prueba de balas contra distintos tipos de guiones negativos.
        """
        if isinstance(valor, (int, float)): return float(valor)
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return 0.0

        try:
            # 1. Detectar si el número es negativo ANTES de limpiar símbolos raros
            es_negativo = False
            if '-' in val_str or '−' in val_str or '–' in val_str or '—' in val_str or ('(' in val_str and ')' in val_str):
                es_negativo = True
            
            # 2. Dejar puramente números, puntos y comas
            val_str = re.sub(r'[^\d.,]', '', val_str) 
            
            # 3. Formato Venezuela: (1.000,00) -> (1000.00)
            if '.' in val_str and ',' in val_str:
                val_str = val_str.replace('.', '').replace(',', '.')
            elif ',' in val_str and '.' not in val_str:
                val_str = val_str.replace(',', '.')
            
            # 4. Convertir a Float y aplicar signo
            num = float(val_str)
            return -num if es_negativo else num
        except:
            return 0.0

    def limpiar_referencia(self, valor):
        """
        Limpia referencias quitando .0, espacios y notación científica.
        """
        if pd.isna(valor) or str(valor).strip() == "": return ""

        val_str = str(valor).strip()
        if val_str.endswith(".0"): val_str = val_str[:-2]

        if "E+" in val_str.upper():
            try: val_str = str(int(float(val_str)))
            except Exception: pass

        return val_str

    def procesar_archivo_bvc(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (BVC): {nombre_archivo}")

        # --- DETECCIÓN DE MONEDA ---
        es_usd = any(x in nombre_archivo.lower() for x in ['usd', 'dolar', 'divisa', 'verde', 'extranjera'])

        if not os.path.exists(archivo_path): return pd.DataFrame()

        try:
            try:
                df_raw = pd.read_excel(archivo_path, header=None)
            except Exception:
                try: df_raw = pd.read_csv(archivo_path, header=None, sep=";", encoding="latin1")
                except Exception:
                    print("❌ No se pudo leer el archivo BVC.")
                    return pd.DataFrame()

            fila_inicio = None

            # --- Buscador de Encabezados ---
            print("   🕵️  Buscando encabezados (BVC)...")
            for i in range(min(30, len(df_raw))):
                fila_vals = df_raw.iloc[i].astype(str).fillna("").tolist()
                fila_str = " ".join(fila_vals).lower()

                # Criterio BVC
                if "fecha" in fila_str and \
                   any(x in fila_str for x in ["abono", "abonos", "credito", "crédito", "monto", "importe"]) and \
                   any(x in fila_str for x in ["cargo", "cargos", "debito", "débito", "saldo"]):
                    fila_inicio = i
                    break

            if fila_inicio is None:
                print("❌ No se encontraron encabezados válidos para BVC.")
                return pd.DataFrame()

            if archivo_path.lower().endswith(".csv"):
                df = pd.read_csv(archivo_path, header=fila_inicio, sep=";", encoding="latin1")
            else:
                df = pd.read_excel(archivo_path, header=fila_inicio)

            df.columns = [str(col).strip() for col in df.columns]

            # --- MAPEO DE COLUMNAS ---
            col_fecha = next((c for c in df.columns if "fecha" in c.lower()), None)
            
            col_ref = next((c for c in df.columns if c.lower() == "referencia"), None)
            if not col_ref: col_ref = next((c for c in df.columns if "referencia" in c.lower()), None)
            if not col_ref: col_ref = next((c for c in df.columns if any(x in c.lower() for x in ["nro", "documento", "doc", "ref"])), None)

            col_debito = next((c for c in df.columns if any(x in c.lower() for x in ["cargo", "cargos", "debito", "débito", "egreso"])), None)
            col_credito = next((c for c in df.columns if any(x in c.lower() for x in ["abono", "abonos", "credito", "crédito", "ingreso"])), None)
            col_monto = next((c for c in df.columns if any(x in c.lower() for x in ['monto', 'importe'])), None)
            
            col_saldo = next((c for c in df.columns if any(x in c.lower() for x in ["saldo", "balance"])), "Saldo")
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ["detalle", "descrip", "concepto", "glosa"])), "Descripción")

            if not col_fecha:
                print("❌ No se encontró columna de fecha en BVC.")
                return pd.DataFrame()

            filas_normalizadas = []

            for idx, fila in df.iterrows():
                try:
                    fecha_val = fila.get(col_fecha)
                    fecha_obj = pd.to_datetime(fecha_val, errors="coerce", dayfirst=True)
                    if pd.isna(fecha_obj): continue

                    semana_str = obtener_semana_corte_viernes(fecha_obj)
                    try: num_semana = int(semana_str.split()[1])
                    except Exception: num_semana = 1

                    monto_bs = 0.0
                    tipo_op = "DEBITO"
                    
                    if col_debito and col_credito:
                        debito = self.limpiar_numero(fila.get(col_debito, 0))
                        credito = self.limpiar_numero(fila.get(col_credito, 0))
                        
                        if abs(debito) > 0:
                            monto_bs = -abs(debito)
                            tipo_op = "DEBITO"
                        elif abs(credito) > 0:
                            monto_bs = abs(credito)
                            tipo_op = "CREDITO"
                    elif col_monto:
                        val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                        if val_monto < 0:
                            monto_bs = -abs(val_monto)
                            tipo_op = "DEBITO"
                        else:
                            monto_bs = abs(val_monto)
                            tipo_op = "CREDITO"

                    if monto_bs == 0: continue

                    # --- LÓGICA DE MONEDA Y BANCO EXACTA ---
                    if es_usd:
                        moneda_ref = 'US'               
                        banco_nombre = 'BVC_USD'        
                        tasa = 1.0
                        monto_usd = monto_bs
                    else:
                        moneda_ref = 'BS'               
                        banco_nombre = 'BVC'            
                        tasa = self.gestor_tasas.obtener_tasa(fecha_obj)
                        monto_usd = round(monto_bs / tasa, 2) if tasa > 0 else 0.0

                    descripcion = str(fila.get(col_desc, "")).strip() if col_desc else ""
                    referencia = self.limpiar_referencia(fila.get(col_ref, "")) if col_ref else ""
                    saldo = self.limpiar_numero(fila.get(col_saldo, 0))

                    fila_norm = {
                        "Fecha": fecha_obj.strftime("%d-%m-%Y"),
                        "Fecha_DT": fecha_obj,
                        "Año": fecha_obj.year,
                        "Mes": fecha_obj.month,
                        "Semana": num_semana,
                        "Referencia Bancaria": referencia,
                        "Descripcion": descripcion,
                        "Monto REF": monto_bs,
                        "Saldo REF": saldo,
                        "Moneda REF": moneda_ref,
                        "Tasa de Cambio": tasa,
                        "Monto USD": monto_usd,
                        "Concepto EY": "",
                        "Proveedor/Cliente": "",
                        "Tipo de Operacion": tipo_op,
                        "Banco": banco_nombre,
                    }
                    filas_normalizadas.append(fila_norm)
                except Exception: continue

            return pd.DataFrame(filas_normalizadas)

        except Exception as e:
            print(f"❌ Error procesando archivo BVC {nombre_archivo}: {e}")
            return pd.DataFrame()

    def guardar_archivo(self, df, nombre_base="BVC"):
        if df.empty: return False

        df_save = df.drop(columns=["Fecha_DT"], errors="ignore")
        fechas = pd.to_datetime(df["Fecha"], format="%d-%m-%Y")
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/bvc/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True

        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print("\n🚀 --- Iniciando Módulo: BVC ---")

    normalizador = NormalizadorBVC(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed",
    )

    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="BVC")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    if not archivos_validos:
        print("⚠️ No se encontraron archivos de BVC.")
        return False

    lista_dfs = []
    for ruta in archivos_validos:
        df_temp = normalizador.procesar_archivo_bvc(ruta)
        if not df_temp.empty:
            moneda = df_temp['Moneda REF'].iloc[0] if not df_temp.empty else 'BS'
            lista_dfs.append({
                "df": df_temp,
                "min_fecha": df_temp["Fecha_DT"].min(),
                "nombre": os.path.basename(ruta),
                "moneda": moneda
            })

    if not lista_dfs:
        print("❌ No se obtuvieron datos válidos para BVC.")
        return False

    # Agrupar estrictamente por Moneda
    agrupado = {}
    for item in lista_dfs:
        m = item['moneda']
        if m not in agrupado: agrupado[m] = []
        agrupado[m].append(item)
        
    dataframes_finales = []
    
    for moneda, grupo in agrupado.items():
        grupo.sort(key=lambda x: x['min_fecha'])
        
        print(f"   ✂️  Verificando solapamientos ({moneda})...")
        for i in range(len(grupo) - 1):
            actual = grupo[i]
            siguiente = grupo[i+1]
            fecha_inicio_siguiente = siguiente['df']['Fecha_DT'].min()
            filas_antes = len(actual['df'])
            
            actual['df'] = actual['df'][actual['df']['Fecha_DT'] < fecha_inicio_siguiente]
            
            diff = filas_antes - len(actual['df'])
            if diff > 0: print(f"      ✂️  Recortadas {diff} filas de '{actual['nombre']}'.")
        
        df_grupo = pd.concat([g['df'] for g in grupo], ignore_index=True)
        dataframes_finales.append(df_grupo)

    if not dataframes_finales: return False
    
    df_total_consolidado = pd.concat(dataframes_finales, ignore_index=True)

    print("\n💾 Guardando archivos consolidados (Separados por Mes y Moneda)...")
    exito = True
    for (mes, moneda), grupo_df in df_total_consolidado.groupby(['Mes', 'Moneda REF']):
        sufijo_moneda = "USD" if moneda == 'US' else "BS"
        nombre_base = f"BVC_{sufijo_moneda}_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo_df, nombre_base): exito = False

    return exito


if __name__ == "__main__":
    print("🔧 Ejecución manual BVC...")
    scanner = ScannerAutomatico(subcarpeta_datos="Ed_cuenta_Yolo_mar")
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)