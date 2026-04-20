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

class NormalizadorBNC:
    def __init__(self, gestor_tasas, carpeta_salida="output"):
        self.gestor_tasas = gestor_tasas
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        os.makedirs(f"{self.carpeta_salida}/bnc", exist_ok=True)
    
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
        if pd.isna(valor) or str(valor).strip() == '': return ''
        val_str = str(valor).strip()
        if val_str.endswith('.0'): val_str = val_str[:-2]
        return val_str

    def procesar_archivo_bnc(self, archivo_path):
        nombre_archivo = os.path.basename(archivo_path)
        print(f"🔄 Leyendo archivo (BNC): {nombre_archivo}")
        
        if not os.path.exists(archivo_path): return pd.DataFrame()

        try:
            # 1. Leer primeras filas para detectar Dólares/Moneda
            try:
                df_raw = pd.read_excel(archivo_path, header=None)
            except:
                try: df_raw = pd.read_csv(archivo_path, header=None, sep=';', encoding='latin1')
                except: return pd.DataFrame()

            texto_inicial = ' '.join(df_raw.iloc[0:15].astype(str).values.flatten()).lower()
            es_dolares = any(kw in texto_inicial for kw in ['$', 'usd', 'moneda extranjera', 'libre convertibilidad', 'dolares', 'dólares'])

            fila_inicio = None
            
            # 2. Buscador de encabezados
            print("   🕵️  Buscando encabezados...")
            for i in range(min(40, len(df_raw))): 
                fila_vals = df_raw.iloc[i].astype(str).fillna('').tolist()
                fila_str = ' '.join(fila_vals).lower()
                
                # Regla más laxa: Debe tener 'fecha' y al menos algo de montos o saldos
                if 'fecha' in fila_str and \
                   any(x in fila_str for x in ['monto', 'cargo', 'abono', 'retiro', 'deposito', 'saldo', 'crédito', 'débito', 'debe', 'haber']):
                    fila_inicio = i
                    print(f"   ✅ Encabezados encontrados en la fila {i}")
                    break
            
            if fila_inicio is None:
                print(f"❌ No se encontraron encabezados válidos.")
                return pd.DataFrame()

            if archivo_path.endswith('.csv'):
                 df = pd.read_csv(archivo_path, header=fila_inicio, sep=';', encoding='latin1')
            else:
                 df = pd.read_excel(archivo_path, header=fila_inicio)

            # Limpiar saltos de línea molestos en los encabezados (ej: "Código\n Transacción")
            df.columns = [str(col).replace('\n', ' ').strip() for col in df.columns]
            
            # --- MAPEO DE COLUMNAS (Actualizado con Debe/Haber) ---
            col_fecha = next((c for c in df.columns if 'fecha' in c.lower()), None)
            col_cargo = next((c for c in df.columns if any(x in c.lower() for x in ['cargo', 'retiro', 'débito', 'debito', 'debe'])), None)
            col_abono = next((c for c in df.columns if any(x in c.lower() for x in ['abono', 'depósito', 'deposito', 'crédito', 'credito', 'haber'])), None)
            col_monto = next((c for c in df.columns if any(x in c.lower() for x in ['monto', 'importe'])), None)
            
            col_desc = next((c for c in df.columns if any(x in c.lower() for x in ['concepto', 'descripción', 'descripcion', 'detalle'])), 'Descripción')
            col_ref = next((c for c in df.columns if any(x in c.lower() for x in ['referencia', 'ref', 'doc', 'comprobante'])), 'Referencia')
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
                    
                    val_absoluto = 0.0
                    tipo_op = "DEBITO"
                    factor = 1.0
                    
                    if col_cargo and col_abono:
                        cargo = self.limpiar_numero(fila.get(col_cargo, 0))
                        abono = self.limpiar_numero(fila.get(col_abono, 0))
                        
                        if cargo > 0:
                            val_absoluto = cargo
                            tipo_op = "DEBITO"
                            factor = -1.0
                        elif abono > 0:
                            val_absoluto = abono
                            tipo_op = "CREDITO"
                            factor = 1.0
                    elif col_monto:
                        val_monto = self.limpiar_numero(fila.get(col_monto, 0))
                        if val_monto < 0:
                            val_absoluto = abs(val_monto)
                            tipo_op = "DEBITO"
                            factor = -1.0
                        else:
                            val_absoluto = val_monto
                            tipo_op = "CREDITO"
                            factor = 1.0

                    if val_absoluto == 0: continue

                    monto_bs = val_absoluto * factor

                    # --- LÓGICA DE MONEDA Y BANCO EXACTA ---
                    if es_dolares:
                        moneda_ref = 'US'        
                        banco_nombre = 'BNC_USD' 
                        tasa = 1.0
                        monto_usd = monto_bs
                    else:
                        moneda_ref = 'BS'        
                        banco_nombre = 'BNC'     
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

    def guardar_archivo(self, df, nombre_base="BNC"):
        if df.empty: return False
        df_save = df.drop(columns=['Fecha_DT'], errors='ignore')
        fechas = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
        if not fechas.empty:
            rango = f"{fechas.min().strftime('%d-%m')} al {fechas.max().strftime('%d-%m')}"
            path = f"{self.carpeta_salida}/bnc/{nombre_base} [{rango}].xlsx"
            df_save.to_excel(path, index=False)
            print(f"💾 Guardado: {path} ({len(df)} filas)")
            return True
        return False

# -----------------------------------------------------------------------------
# ✅ ORQUESTADOR
# -----------------------------------------------------------------------------
def ejecutar_normalizacion(scanner_global, gestor_tasas_global):
    print(f"\n🚀 --- Iniciando Módulo: BNC ---")
    
    normalizador = NormalizadorBNC(
        gestor_tasas=gestor_tasas_global,
        carpeta_salida="estados_cuenta_processed"
    )

    archivos_encontrados = scanner_global.listar_archivos_simples(criterio_nombre="BNC")
    archivos_validos = [f for f in archivos_encontrados if not os.path.basename(f).startswith("~$")]

    if not archivos_validos:
        print("⚠️ No se encontraron archivos de BNC.")
        return False

    lista_dfs = []
    for ruta in archivos_validos:
        
        # --- FILTRO CRÍTICO ---
        # Si el archivo contiene "6550", lo saltamos
        if "6550" in os.path.basename(ruta):
            print(f"   ⚠️ Saltando archivo BNC 6550: {os.path.basename(ruta)}")
            continue
        # ----------------------

        df_temp = normalizador.procesar_archivo_bnc(ruta)
        if not df_temp.empty:
            moneda = df_temp['Moneda REF'].iloc[0]
            lista_dfs.append({
                'df': df_temp,
                'min_fecha': df_temp['Fecha_DT'].min(),
                'nombre': os.path.basename(ruta),
                'moneda': moneda
            })
    
    if not lista_dfs: 
        print("❌ No se obtuvieron datos válidos para BNC.")
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
            if diff > 0:
                print(f"      ✂️  Recortadas {diff} filas de '{actual['nombre']}'.")
        
        df_grupo = pd.concat([g['df'] for g in grupo], ignore_index=True)
        dataframes_finales.append(df_grupo)

    if not dataframes_finales: return False
    
    df_total_consolidado = pd.concat(dataframes_finales, ignore_index=True)

    print("\n💾 Guardando archivos consolidados (Separados por Mes y Moneda)...")
    exito = True
    
    for (mes, moneda), grupo_df in df_total_consolidado.groupby(['Mes', 'Moneda REF']):
        nombre_base = f"BNC_{moneda}_Mes_{mes}"
        if not normalizador.guardar_archivo(grupo_df, nombre_base): exito = False
            
    return exito

if __name__ == "__main__":
    print("🔧 Ejecución manual...")
    scanner = ScannerAutomatico(subcarpeta_datos="Ed_cuenta_Yolo_mar")
    ruta_tasas = scanner.obtener_ruta_tasas()
    gestor = GestorTasas(ruta_tasas)
    ejecutar_normalizacion(scanner, gestor)