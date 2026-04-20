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

class NormalizadorMercantil:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        os.makedirs(f"{self.carpeta_salida}/mercantil", exist_ok=True)
    
    def limpiar_numero(self, valor):
        """Limpieza a prueba de balas para signos negativos y formatos."""
        if isinstance(valor, (int, float)): return float(valor)
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return 0.0

        try:
            es_negativo = False
            if '-' in val_str or '−' in val_str or '–' in val_str or '—' in val_str or ('(' in val_str and ')' in val_str):
                es_negativo = True

            val_str = re.sub(r'[^\d.,]', '', val_str) 
            
            # Formato Venezuela (1.000,00) -> Python (1000.00)
            if '.' in val_str and ',' in val_str:
                val_str = val_str.replace('.', '').replace(',', '.')
            elif ',' in val_str and '.' not in val_str:
                val_str = val_str.replace(',', '.')
            
            numero = float(val_str)
            return -numero if es_negativo else numero
        except:
            return 0.0

    def limpiar_referencia(self, valor):
        if pd.isna(valor) or str(valor).strip() == '': return ''
        val_str = str(valor).strip()
        if val_str.endswith('.0'): val_str = val_str[:-2]
        return val_str

    # 🚀 Agregado: Detector inteligente de fechas para evitar futuros dolores de cabeza
    def limpiar_fecha_espanol(self, fecha_str):
        if pd.isna(fecha_str): return pd.NaT
        if isinstance(fecha_str, datetime): return fecha_str
        
        s = str(fecha_str).lower().strip()
        
        meses = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        
        for es, num in meses.items():
            if es in s:
                s = s.replace(f" de {es} de ", f"/{num}/")
                s = s.replace(f" {es} ", f"/{num}/")
                s = s.replace(f"-{es[:3]}-", f"/{num}/")
                
        s = s.split()[0]
        
        try:
            if re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}', s):
                return pd.to_datetime(s, errors='coerce', yearfirst=True)
            else:
                return pd.to_datetime(s, errors='coerce', dayfirst=True)
        except:
            return pd.NaT

    def procesar_archivo_mercantil(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (Mercantil): {nombre_archivo}")
        
        # 🚀 CORRECCIÓN: Usamos Regex para detectar palabras exactas y evitar que "US" pase como "BS"
        es_usd = bool(re.search(r'\b(usd|us|dolar|dólar|dolares|dólares|divisa|divisas|verde|panama)\b', nombre_archivo.lower()))

        if not os.path.exists(archivo_path): return pd.DataFrame()

        try:
            try:
                df_raw = pd.read_excel(archivo_path, header=None)
            except:
                try: df_raw = pd.read_csv(archivo_path, header=None, sep=';', encoding='latin1')
                except: return pd.DataFrame()

            fila_inicio = None
            
            print("   🕵️  Buscando encabezados...")
            for i in range(min(25, len(df_raw))): 
                fila_vals = df_raw.iloc[i].astype(str).fillna('').tolist()
                fila_str = ' '.join(fila_vals).lower()
                
                if 'fecha' in fila_str and \
                   any(x in fila_str for x in ['monto', 'haber', 'debe', 'saldo', 'tipo', 'movimiento', 'transaccion', 'descripción', 'concepto']):
                    fila_inicio = i
                    break
            
            if fila_inicio is None:
                print(f"❌ No se encontraron encabezados válidos.")
                return pd.DataFrame()

            if archivo_path.endswith('.csv'):
                 df = pd.read_csv(archivo_path, header=fila_inicio, sep=';', encoding='latin1')
            else:
                 df = pd.read_excel(archivo_path, header=fila_inicio)

            df.columns = [str(col).strip() for col in df.columns]
            
            col_fecha = next((c for c in df.columns if 'fecha' in c.lower()), None)
            col_monto = next((c for c in df.columns if any(x in c.lower() for x in ['monto', 'importe'])), None)
            col_saldo = next((c for c in df.columns if any(x in c.lower() for x in ['saldo', 'balance'])), 'Saldo')
            col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['ref', 'documento', 'doc', 'número'])), 'Referencia')
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ['desc', 'concepto', 'detalle'])), 'Concepto')

            filas_normalizadas = []
            
            for idx, fila in df.iterrows():
                try:
                    fecha_val = fila.get(col_fecha)
                    fecha_obj = self.limpiar_fecha_espanol(fecha_val)
                    
                    if pd.isna(fecha_obj): continue
                    
                    semana_str = obtener_semana_corte_viernes(fecha_obj)
                    try: num_semana = int(semana_str.split()[1])
                    except: num_semana = 1
                    
                    val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                    val_absoluto = abs(val_monto)

                    if val_absoluto == 0: continue

                    if val_monto < 0:
                        monto_bs = val_monto
                        tipo_op = "DEBITO"
                    else:
                        monto_bs = val_monto
                        tipo_op = "CREDITO"

                    # --- LÓGICA DE MONEDA EXACTA ---
                    if es_usd:
                        moneda_ref = 'US'               
                        banco_nombre = 'MERCANTIL_USD'    
                        tasa = 1.0
                        monto_usd = monto_bs
                    else:
                        moneda_ref = 'BS'               
                        banco_nombre = 'MERCANTIL'        
                        tasa = self.gestor_tasas.obtener_tasa(fecha_obj)
                        monto_usd = round(monto_bs / tasa, 2) if tasa > 0 else 0.0
                        
                    descripcion = str(fila.get(col_desc, '')).strip()
                    referencia = self.limpiar_referencia(fila.get(col_ref, ''))
                    saldo = self.limpiar_numero(fila.get(col_saldo, 0))
                    
                    fila_norm = {
                        'Fecha': fecha_obj.strftime('%d-%m-%Y'),
                        'Fecha_DT': fecha_obj, 
                        'Año': fecha_obj.year,
                        'Mes': fecha_obj.month,
                        'Semana': num_semana,
                        'Referencia Bancaria': referencia,
                        'Descripcion': descripcion,
                        'Monto REF': monto_bs,      
                        'Saldo REF': saldo,          
                        'Moneda REF': moneda_ref,
                        'Tasa de Cambio': tasa,
                        'Monto USD': monto_usd,     
                        'Concepto EY': '',
                        'Proveedor/Cliente': '',    
                        'Tipo de Operacion': tipo_op,
                        'Banco': banco_nombre
                    }
                    filas_normalizadas.append(fila_norm)
                except Exception: continue

            return pd.DataFrame(filas_normalizadas)

        except Exception as e:
            print(f"❌ Error procesando archivo {nombre_archivo}: {e}")
            return pd.DataFrame()

    def guardar_archivo(self, df, nombre_base="Mercantil"):
        if df.empty: return False
        df_save = df.drop(columns=['Fecha_DT'], errors='ignore')
        fechas = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/mercantil/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True
        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print(f"\n🚀 --- Iniciando Módulo: MERCANTIL ---")
    
    normalizador = NormalizadorMercantil(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed"
    )

    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="MERCANTIL")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    if not archivos_validos:
        print("⚠️ No se encontraron archivos de Mercantil.")
        return False

    lista_dfs = []
    for ruta in archivos_validos:
        nombre_archivo = os.path.basename(ruta).upper()
        
        if "PANAMA" in nombre_archivo or "PANAMÁ" in nombre_archivo:
            print(f"   ⚠️ Saltando archivo de PANAMÁ (usar MercantilPan_Norm.py): {nombre_archivo}")
            continue

        df_temp = normalizador.procesar_archivo_mercantil(ruta)
        if not df_temp.empty:
            moneda = df_temp['Moneda REF'].iloc[0]
            lista_dfs.append({
                'df': df_temp,
                'min_fecha': df_temp['Fecha_DT'].min(),
                'nombre': os.path.basename(ruta),
                'moneda': moneda
            })
    
    if not lista_dfs: 
        print("❌ No se obtuvieron datos válidos.")
        return False

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
        nombre_base = f"Mercantil_{sufijo_moneda}_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo_df, nombre_base): exito = False
            
    return exito

if __name__ == "__main__":
    print("🔧 Ejecución manual...")
    scanner = ScannerAutomatico(subcarpeta_datos="Ed_cuenta_Yolo")
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)