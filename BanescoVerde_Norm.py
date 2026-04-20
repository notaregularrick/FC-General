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

class NormalizadorBanescoVerde:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        os.makedirs(f"{self.carpeta_salida}/banesco_verde", exist_ok=True)
    
    def limpiar_numero(self, valor):
        """
        Limpieza robusta para formato Venezuela (1.000,00) -> Float.
        Aunque sea Cuenta Verde (USD), Banesco Venezuela suele usar formato local.
        """
        if isinstance(valor, (int, float)): return float(valor)
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return 0.0

        try:
            val_str = re.sub(r'[^\d.,-]', '', val_str) 
            if '(' in str(valor) and ')' in str(valor): val_str = '-' + val_str
            
            # Formato Venezuela: 
            if '.' in val_str and ',' in val_str:
                val_str = val_str.replace('.', '').replace(',', '.')
            elif ',' in val_str and '.' not in val_str:
                val_str = val_str.replace(',', '.')
            
            return float(val_str)
        except:
            return 0.0

    def limpiar_referencia(self, valor):
        if pd.isna(valor) or str(valor).strip() == '': return ''
        val_str = str(valor).strip()
        if val_str.endswith('.0'): val_str = val_str[:-2]
        return val_str

    def procesar_archivo_banesco_verde(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (Banesco Verde): {nombre_archivo}")
        
        if not os.path.exists(archivo_path): return pd.DataFrame()

        try:
            try:
                df_raw = pd.read_excel(archivo_path, header=None)
            except:
                try:
                    df_raw = pd.read_csv(archivo_path, header=None, sep=';', encoding='latin1')
                except:
                    return pd.DataFrame()

            fila_inicio = None
            
            # --- Buscador de Encabezados ---
            print("   🕵️  Buscando encabezados...")
            for i in range(min(25, len(df_raw))): 
                fila_vals = df_raw.iloc[i].astype(str).fillna('').tolist()
                fila_str = ' '.join(fila_vals).lower()
                
                # Buscamos 'fecha' y 'monto', y opcionalmente 'd/c'
                if 'fecha' in fila_str and \
                   any(x in fila_str for x in ['monto', 'importe', 'd/c', 'signo', 'tipo']):
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
            print(f"   🐛 Columnas: {df.columns.tolist()}")
            
            # --- MAPEO DE COLUMNAS ---
            col_fecha = next((c for c in df.columns if 'fecha' in c.lower()), None)
            col_monto = next((c for c in df.columns if any(x in c.lower() for x in ['monto', 'importe'])), None)
            
            # --- DETECCIÓN CRÍTICA: COLUMNA D/C ---
            col_dc = next((c for c in df.columns if any(x in c.lower() for x in ['d/c', 'signo', 'db/cr'])), None)
            
            if col_dc:
                print(f"   ✅ Columna de Signo (D/C) detectada: '{col_dc}'")
            else:
                print("   ⚠️ ADVERTENCIA: No se detectó columna 'D/C'. Se intentará inferir.")

            col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['referencia', 'ref', 'doc'])), 'Referencia')
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ['desc', 'concepto', 'detalle'])), 'Descripción')
            col_saldo = next((c for c in df.columns if any(x in c.lower() for x in ['saldo', 'balance'])), 'Saldo')

            filas_normalizadas = []
            
            for idx, fila in df.iterrows():
                try:
                    fecha_val = fila.get(col_fecha)
                    fecha_obj = pd.to_datetime(fecha_val, errors='coerce', dayfirst=True)
                    if pd.isna(fecha_obj): continue
                    
                    semana_str = obtener_semana_corte_viernes(fecha_obj)
                    try: num_semana = int(semana_str.split()[1])
                    except: num_semana = 1
                    
                    val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                    val_absoluto = abs(val_monto)

                    if val_absoluto == 0: continue

                    # --- LÓGICA DE SIGNOS BASADA EN COLUMNA D/C ---
                    signo_detectado = "CREDITO" # Default positivo
                    factor = 1.0

                    if col_dc:
                        val_dc = str(fila.get(col_dc, '')).upper().strip()
                        # Si es Débito (D) o signo menos
                        if val_dc in ['D', '-', 'DEBITO', 'DÉBITO', 'DB', 'CARGO']:
                            signo_detectado = "DEBITO"
                            factor = -1.0
                        # Si es Crédito (C) o signo mas
                        elif val_dc in ['C', '+', 'CREDITO', 'CRÉDITO', 'CR', 'ABONO']:
                            signo_detectado = "CREDITO"
                            factor = 1.0
                    
                    monto_bs = val_absoluto * factor
                    tipo_op = signo_detectado

                    # Cuenta Verde es USD
                    moneda_ref = 'US'
                    tasa = 1.0
                    monto_usd = monto_bs # El monto ya está en dólares
                        
                    descripcion = str(fila.get(col_desc, '')).strip()
                    referencia = self.limpiar_referencia(fila.get(col_ref, ''))
                    saldo = self.limpiar_numero(fila.get(col_saldo, 0))

                    fila_norm = {
                        'Fecha': fecha_obj.strftime('%d-%m-%Y'),
                        'Fecha_DT': fecha_obj, 
                        'Año': fecha_obj.year,  # <--- ✅ AÑO
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
                        'Banco': 'BANESCOVERDE'
                    }
                    filas_normalizadas.append(fila_norm)
                except Exception: continue

            return pd.DataFrame(filas_normalizadas)

        except Exception as e:
            print(f"❌ Error procesando archivo {nombre_archivo}: {e}")
            return pd.DataFrame()

    def guardar_archivo(self, df, nombre_base="BanescoVerde"):
        if df.empty: return False
        df_save = df.drop(columns=['Fecha_DT'], errors='ignore')
        fechas = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/banesco_verde/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True
        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print(f"\n🚀 --- Iniciando Módulo: BANESCO VERDE ---")
    
    normalizador = NormalizadorBanescoVerde(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed"
    )

    # Buscar archivos "VERDE"
    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="VERDE")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    # Filtro Extra: Asegurar que sea BANESCO
    archivos_banesco_verde = [f for f in archivos_validos if "BANESCO" in os.path.basename(f).upper()]

    if not archivos_banesco_verde:
        print("⚠️ No se encontraron archivos de Banesco Verde.")
        return False

    lista_dfs = []
    for ruta in archivos_banesco_verde:
        df_temp = normalizador.procesar_archivo_banesco_verde(ruta)
        if not df_temp.empty:
            lista_dfs.append({
                'df': df_temp,
                'min_fecha': df_temp['Fecha_DT'].min(),
                'nombre': os.path.basename(ruta)
            })
    
    if not lista_dfs: 
        print("❌ No se obtuvieron datos válidos.")
        return False

    # Ordenar y Recortar Solapamientos
    lista_dfs.sort(key=lambda x: x['min_fecha'])
    print(f"\n✂️  Verificando solapamientos (Banesco Verde)...")
    for i in range(len(lista_dfs) - 1):
        actual = lista_dfs[i]
        siguiente = lista_dfs[i+1]
        fecha_inicio_siguiente = siguiente['df']['Fecha_DT'].min()
        filas_antes = len(actual['df'])
        
        actual['df'] = actual['df'][actual['df']['Fecha_DT'] < fecha_inicio_siguiente]
        
        diff = filas_antes - len(actual['df'])
        if diff > 0: print(f"   ✂️  Recortadas {diff} filas de '{actual['nombre']}'.")

    df_total = pd.concat([item['df'] for item in lista_dfs], ignore_index=True)
    
    print("\n💾 Guardando archivos consolidados por mes...")
    exito = True
    for mes, grupo in df_total.groupby('Mes'):
        nombre_base = f"BanescoVerde_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo, nombre_base): exito = False
            
    return exito

if __name__ == "__main__":
    print("🔧 Ejecución manual...")
    scanner = ScannerAutomatico(subcarpeta_datos="carpeta de origen") #poner el nombre de la carpeta de origen personalizada o borrar para que agarre la default     
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)