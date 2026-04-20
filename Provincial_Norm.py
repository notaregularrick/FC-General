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

class NormalizadorProvincial:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        os.makedirs(f"{self.carpeta_salida}/provincial", exist_ok=True)
    
    def limpiar_numero(self, valor):
        """Limpieza robusta para formato Venezuela (1.000,00) -> Float"""
        if isinstance(valor, (int, float)): return float(valor)
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return 0.0

        try:
            val_str = re.sub(r'[^\d.,-]', '', val_str) 
            if '(' in str(valor) and ')' in str(valor): val_str = '-' + val_str
            
            # Formato Venezuela
            if '.' in val_str and ',' in val_str:
                val_str = val_str.replace('.', '').replace(',', '.')
            elif ',' in val_str and '.' not in val_str:
                val_str = val_str.replace(',', '.')
            
            return float(val_str)
        except:
            return 0.0

    def limpiar_referencia(self, valor):
        """Limpia referencias quitando .0 y espacios"""
        if pd.isna(valor) or str(valor).strip() == '': return ''
        val_str = str(valor).strip()
        if val_str.endswith('.0'): val_str = val_str[:-2]
        return val_str

    def procesar_archivo_provincial(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (Provincial): {nombre_archivo}")
        
        # Detección de moneda por nombre
        es_usd = any(x in nombre_archivo.lower() for x in ['usd', 'divisa', 'extranjera', 'dolar'])

        if not os.path.exists(archivo_path): return pd.DataFrame()

        try:
            try:
                df_raw = pd.read_excel(archivo_path, header=None)
            except:
                try: df_raw = pd.read_csv(archivo_path, header=None, sep=';', encoding='latin1')
                except: return pd.DataFrame()

            fila_inicio = None
            
            # --- Buscador de Encabezados (CORREGIDO PARA FILA 17) ---
            print("   🕵️  Buscando encabezados...")
            # Buscamos hasta la fila 40 por seguridad
            for i in range(min(40, len(df_raw))): 
                fila_vals = df_raw.iloc[i].astype(str).fillna('').tolist()
                fila_str = ' '.join(fila_vals).lower()
                
                # LA CLAVE: Debe tener 'importe' Y ('operación' O 'valor' O 'fecha')
                # Esto evita confundirse con la fila 9 que solo dice "fecha contable"
                if 'importe' in fila_str and \
                   any(x in fila_str for x in ['operación', 'operacion', 'valor', 'fecha']):
                    fila_inicio = i
                    break
            
            if fila_inicio is None:
                print(f"❌ No se encontraron encabezados válidos (Se buscó 'Importe' + 'Operación/Fecha').")
                return pd.DataFrame()

            # Recargar con header correcto
            if archivo_path.endswith('.csv'):
                 df = pd.read_csv(archivo_path, header=fila_inicio, sep=';', encoding='latin1')
            else:
                 df = pd.read_excel(archivo_path, header=fila_inicio)

            df.columns = [str(col).strip() for col in df.columns]
            
            # --- MAPEO DE COLUMNAS ---
            # Fecha: Busca "F. Operación" o "Fecha"
            col_fecha = next((c for c in df.columns if any(x in c.lower() for x in ['operación', 'operacion', 'fecha'])), None)
            
            # Referencia: Busca "Nº. Doc." o "Doc" o "Referencia"
            col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['nº. doc', 'doc.', 'referencia'])), None)
            
            # Monto: Busca "Importe" (Provincial usa una sola columna con signo usualmente, o cargos/abonos)
            col_monto = next((c for c in df.columns if 'importe' in c.lower()), None)
            
            # Saldo
            col_saldo = next((c for c in df.columns if 'saldo' in c.lower()), None)
            
            # Descripción: Busca "Concepto"
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ['concepto', 'detalle'])), None)

            if not col_fecha or not col_monto:
                print(f"⚠️ Columnas críticas no encontradas. Fecha: {col_fecha}, Monto: {col_monto}")
                return pd.DataFrame()

            filas_normalizadas = []
            
            for idx, fila in df.iterrows():
                try:
                    fecha_val = fila.get(col_fecha)
                    # Si es texto vacío o NaT, saltar (Esto filtra la fila "Saldo Inicial")
                    if pd.isna(fecha_val) or str(fecha_val).strip() == '': continue

                    fecha_obj = pd.to_datetime(fecha_val, errors='coerce', dayfirst=True)
                    if pd.isna(fecha_obj): continue # Salta filas de resumen sin fecha válida
                    
                    semana_str = obtener_semana_corte_viernes(fecha_obj)
                    try: num_semana = int(semana_str.split()[1])
                    except: num_semana = 1
                    
                    # Provincial "Importe": Negativo es Débito, Positivo es Crédito (generalmente)
                    val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                    
                    if val_monto == 0: continue

                    monto_bs = val_monto
                    if monto_bs < 0:
                        tipo_op = "DEBITO"
                    else:
                        tipo_op = "CREDITO"

                    # --- LÓGICA DE MONEDA Y BANCO EXACTA ---
                    if es_usd:
                        moneda_ref = 'US'               
                        banco_nombre = 'PROVINCIAL_USD'  
                        tasa = 1.0
                        monto_usd = monto_bs
                    else:
                        moneda_ref = 'BS'               
                        banco_nombre = 'PROVINCIAL'      
                        tasa = self.gestor_tasas.obtener_tasa(fecha_obj)
                        monto_usd = round(monto_bs / tasa, 2) if tasa > 0 else 0.0
                        
                    descripcion = str(fila.get(col_desc, '')) if col_desc else ''
                    descripcion = descripcion.strip()
                    
                    if col_ref:
                        referencia = self.limpiar_referencia(fila.get(col_ref, ''))
                    else:
                        referencia = ''
                        
                    saldo = self.limpiar_numero(fila.get(col_saldo, 0)) if col_saldo else 0.0

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

    def guardar_archivo(self, df, nombre_base="Provincial"):
        if df.empty: return False
        df_save = df.drop(columns=['Fecha_DT'], errors='ignore')
        fechas = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/provincial/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True
        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print(f"\n🚀 --- Iniciando Módulo: PROVINCIAL ---")
    
    normalizador = NormalizadorProvincial(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed"
    )

    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="PROVINCIAL")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    if not archivos_validos:
        print("⚠️ No se encontraron archivos de Provincial.")
        return False

    lista_dfs = []
    for ruta in archivos_validos:
        df_temp = normalizador.procesar_archivo_provincial(ruta)
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
        nombre_base = f"Provincial_{sufijo_moneda}_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo_df, nombre_base): exito = False
            
    return exito

if __name__ == "__main__":
    print("🔧 Ejecución manual...")
    scanner = ScannerAutomatico(subcarpeta_datos="carpeta de origen") #poner el nombre de la carpeta de origen personalizada o borrar para que agarre la default     
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)