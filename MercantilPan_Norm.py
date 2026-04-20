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

class NormalizadorMercantilPanama:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        os.makedirs(f"{self.carpeta_salida}/mercantil_panama", exist_ok=True)
    
    def limpiar_numero(self, valor):
        """
        Limpieza robusta para números.
        Mercantil Panamá suele usar punto decimal, pero prevenimos formatos mixtos.
        """
        if isinstance(valor, (int, float)): return float(valor)
        val_str = str(valor).strip()
        if not val_str or val_str.lower() == 'nan': return 0.0

        try:
            val_str = re.sub(r'[^\d.,-]', '', val_str) 
            if '(' in str(valor) and ')' in str(valor): val_str = '-' + val_str
            
            # Lógica de detección de formato
            if '.' in val_str and ',' in val_str:
                # Si hay ambos, asumimos que la coma es miles (formato US/Panama estándar)
                val_str = val_str.replace(',', '')
            elif ',' in val_str and '.' not in val_str:
                # Si solo hay comas, podría ser decimal (formato EU) o miles.
                # En bancos de Panamá es raro que sea decimal la coma, pero por seguridad:
                # Si tiene 3 decimales (ej 1,000) es miles. Si tiene 2 (10,50) es decimal.
                # Simplificación: Asumimos formato US (coma=miles) si no es ambiguo
                val_str = val_str.replace(',', '')
                
            return float(val_str)
        except:
            return 0.0

    def limpiar_referencia(self, valor):
        """Limpia referencias quitando .0 y espacios"""
        if pd.isna(valor) or str(valor).strip() == '': return ''
        val_str = str(valor).strip()
        # Quitar .0 al final
        if val_str.endswith('.0'): val_str = val_str[:-2]
        # Quitar notación científica si aparece
        if 'E+' in val_str:
            try: val_str = str(int(float(val_str)))
            except: pass
        return val_str

    def procesar_archivo_mercantil_panama(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (Mercantil Panamá): {nombre_archivo}")
        
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
                
                if 'fecha' in fila_str and \
                   any(x in fila_str for x in ['monto', 'debito', 'credito', 'saldo', 'importe']):
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
            
            # --- MAPEO DE COLUMNAS ---
            col_fecha = next((c for c in df.columns if 'fecha' in c.lower()), None)
            
            # Referencia (Jerárquica)
            col_ref = None
            col_ref = next((c for c in df.columns if c.lower() == 'referencia'), None)
            if not col_ref: col_ref = next((c for c in df.columns if 'referencia' in c.lower()), None)
            if not col_ref: col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['documento', 'doc'])), None)
            if not col_ref: col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['nro', 'transaccion'])), None)

            # Dinero
            col_debito = next((c for c in df.columns if any(x in c.lower() for x in ['debito', 'débito', 'retiro'])), None)
            col_credito = next((c for c in df.columns if any(x in c.lower() for x in ['credito', 'crédito', 'deposito'])), None)
            col_monto = next((c for c in df.columns if any(x in c.lower() for x in ['monto', 'importe'])), None)
            
            col_saldo = next((c for c in df.columns if any(x in c.lower() for x in ['saldo', 'balance'])), 'Saldo')
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ['desc', 'concepto', 'detalle'])), 'Descripción')

            filas_normalizadas = []
            
            for idx, fila in df.iterrows():
                try:
                    fecha_val = fila.get(col_fecha)
                    fecha_obj = pd.to_datetime(fecha_val, errors='coerce', dayfirst=True)
                    if pd.isna(fecha_obj): continue
                    
                    semana_str = obtener_semana_corte_viernes(fecha_obj)
                    try: num_semana = int(semana_str.split()[1])
                    except: num_semana = 1
                    
                    monto_bs = 0.0
                    tipo_op = "DEBITO"
                    
                    if col_debito and col_credito:
                        debito = self.limpiar_numero(fila.get(col_debito, 0))
                        credito = self.limpiar_numero(fila.get(col_credito, 0))
                        
                        if debito > 0:
                            monto_bs = -abs(debito)
                            tipo_op = "DEBITO"
                        elif credito > 0:
                            monto_bs = abs(credito)
                            tipo_op = "CREDITO"
                    elif col_monto:
                        val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                        if val_monto < 0:
                            monto_bs = val_monto
                            tipo_op = "DEBITO"
                        else:
                            monto_bs = val_monto
                            tipo_op = "CREDITO"

                    if monto_bs == 0: continue

                    # Mercantil Panamá es USD
                    moneda_ref = 'US'
                    tasa = 1.0
                    monto_usd = monto_bs
                        
                    descripcion = str(fila.get(col_desc, '')).strip()
                    
                    if col_ref:
                        referencia = self.limpiar_referencia(fila.get(col_ref, ''))
                    else:
                        referencia = ''
                        
                    saldo = self.limpiar_numero(fila.get(col_saldo, 0))

                    fila_norm = {
                        'Fecha': fecha_obj.strftime('%d-%m-%Y'),
                        'Fecha_DT': fecha_obj, 
                        'Año': fecha_obj.year,   # <--- ✅ AÑO AGREGADO
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
                        'Banco': 'MERCANTILPANAMA'
                    }
                    filas_normalizadas.append(fila_norm)
                except Exception: continue

            return pd.DataFrame(filas_normalizadas)

        except Exception as e:
            print(f"❌ Error procesando archivo {nombre_archivo}: {e}")
            return pd.DataFrame()

    def guardar_archivo(self, df, nombre_base="MercantilPanama"):
        if df.empty: return False
        df_save = df.drop(columns=['Fecha_DT'], errors='ignore')
        fechas = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/mercantil_panama/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True
        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print(f"\n🚀 --- Iniciando Módulo: MERCANTIL PANAMÁ ---")
    
    normalizador = NormalizadorMercantilPanama(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed"
    )

    # Buscar archivos
    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="PANAMA")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    # Filtro extra por si acaso se cuela algo que no sea Mercantil
    # (Depende de cómo nombres tus archivos, pero "Mercantil" suele estar presente)
    archivos_mercantil = [f for f in archivos_validos if "MERCANTIL" in f.upper()]
    
    if not archivos_mercantil:
        # Si no encuentra con "MERCANTIL", probamos solo los de "PANAMA" que no sean Banesco
        archivos_mercantil = [f for f in archivos_validos if "BANESCO" not in f.upper()]

    if not archivos_mercantil:
        print("⚠️ No se encontraron archivos de Mercantil Panamá.")
        return False

    lista_dfs = []
    for ruta in archivos_mercantil:
        df_temp = normalizador.procesar_archivo_mercantil_panama(ruta)
        if not df_temp.empty:
            lista_dfs.append({
                'df': df_temp,
                'min_fecha': df_temp['Fecha_DT'].min(),
                'nombre': os.path.basename(ruta)
            })
    
    if not lista_dfs: 
        print("❌ No se obtuvieron datos válidos.")
        return False

    # Recorte Solapamientos
    lista_dfs.sort(key=lambda x: x['min_fecha'])
    print(f"\n✂️  Verificando solapamientos (Mercantil Panamá)...")
    for i in range(len(lista_dfs) - 1):
        actual = lista_dfs[i]
        siguiente = lista_dfs[i+1]
        fecha_inicio_siguiente = siguiente['df']['Fecha_DT'].min()
        filas_antes = len(actual['df'])
        actual['df'] = actual['df'][actual['df']['Fecha_DT'] < fecha_inicio_siguiente]
        diff = filas_antes - len(actual['df'])
        if diff > 0: print(f"   ✂️  Recortadas {diff} filas de '{actual['nombre']}'.")

    # Guardar
    df_total = pd.concat([item['df'] for item in lista_dfs], ignore_index=True)
    
    print("\n💾 Guardando archivos consolidados por mes...")
    exito = True
    for mes, grupo in df_total.groupby('Mes'):
        nombre_base = f"MercantilPanama_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo, nombre_base): exito = False
            
    return exito

if __name__ == "__main__":
    print("🔧 Ejecución manual...")
    scanner = ScannerAutomatico(subcarpeta_datos="carpeta de origen") #poner el nombre de la carpeta de origen personalizada o borrar para que agarre la default     
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)