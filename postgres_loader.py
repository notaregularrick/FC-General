import pandas as pd
import numpy as np
import os
import sys
import glob
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import Integer, Numeric, String, Date, DateTime, Boolean
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime

# ========== CONFIGURACIÓN ==========
load_dotenv()

db_config = {
    'host': os.getenv("host", "localhost"),
    'port': os.getenv("port", "5432"),
    'database': os.getenv("database", "bancos_test"),
    'user': os.getenv("user", "postgres"),
    'password': os.getenv("password", "tu_password"),
}

ESQUEMA_COLUMNAS = {
    'id': 'SERIAL PRIMARY KEY',
    'fecha': 'DATE',
    'mes': 'INTEGER',
    'semana': 'INTEGER',
    'ano': 'INTEGER',
    'referencia_bancaria': 'TEXT',
    'descripcion': 'TEXT',
    'monto_ref': 'NUMERIC(20,2)',
    'saldo_ref': 'NUMERIC(20,2)',
    'moneda_ref': 'VARCHAR(10)',
    'tasa_cambio': 'NUMERIC(20,4)',
    'monto_usd': 'NUMERIC(20,2)',
    'concepto_ey': 'TEXT',
    'proveedor_cliente': 'TEXT',
    'tipo_operacion': 'VARCHAR(50)',
    'banco': 'VARCHAR(50)',
    'fecha_carga': 'TIMESTAMP WITHOUT TIME ZONE',
    'es_saldo_final': 'BOOLEAN',
    'saldo_final_calculado': 'NUMERIC(20,2)'
}

# ===================================

class PostgreSQLBancoLoader:
    def __init__(self, db_config):
        self.db_config = db_config
        self.engine = None
        self.connect()
    
    def connect(self):
        try:
            user = self.db_config['user']
            password = quote_plus(str(self.db_config['password']))
            host = self.db_config['host']
            port = self.db_config['port']
            database = self.db_config['database']
            
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            self.engine = create_engine(connection_string)
            print(f"✅ Conexión establecida con BD: {database}")
        except Exception as e:
            print(f"❌ Error crítico de conexión: {e}")
            sys.exit(1)

    def determinar_nombre_tabla(self, nombre_archivo):
        nombre_lower = os.path.basename(nombre_archivo).lower()
        
        # --- LÓGICA BNC (Corregida con prioridades) ---
        if "bnc" in nombre_lower:
            # Prioridad 1: Moneda explícita en el nombre del archivo
            if "6550" in nombre_lower: 
                return "bnc6550_bs_norm"
            if "_bs_" in nombre_lower: 
                return "bnc_bs_norm"
            if "_usd_" in nombre_lower or "_us_" in nombre_lower: 
                return "bnc_us_norm"
            
                
            # Prioridad 2: Por nombre de cuenta (Fallback si no dice la moneda)
            if "principal" in nombre_lower: 
                return "bnc_us_norm"
            if "6550" in nombre_lower: 
                return "bnc6550_bs_norm"
                
            return "bnc_bs_norm" # Default BNC

        # BANESCO
        if "banesco" in nombre_lower:
            if "panama" in nombre_lower: return "banescopanama_us_norm"
            if "verde" in nombre_lower: return "banescoverde_us_norm"
            if "planta" in nombre_lower: return "banescoplanta_bs_norm"
            return "banesco99_bs_norm"

        # OTROS
        banco = "desconocido"
        if "bancamiga" in nombre_lower: banco = "bancamiga"
        elif "mercantil" in nombre_lower:
            if "panama" in nombre_lower: banco = "mercantilpanama"
            else: banco = "mercantil"
        elif "provincial" in nombre_lower: banco = "provincial"
        elif "bdv" in nombre_lower or "venezuela" in nombre_lower: banco = "bdv"
        elif "banplus" in nombre_lower: banco = "banplus"
        else: banco = nombre_lower.split('_')[0]

        # Prioridad general de moneda
        if "_bs_" in nombre_lower:
            moneda = "bs"
        elif any(x in nombre_lower for x in ['usd', 'us', 'dolar', 'divisa', 'verde', 'panama']):
            moneda = "us"
        else:
            moneda = "bs"
            
        return f"{banco}_{moneda}_norm"

    def asegurar_tabla_y_columnas(self, nombre_tabla):
        inspector = inspect(self.engine)
        
        if not inspector.has_table(nombre_tabla):
            print(f"   🔨 Creando tabla nueva: {nombre_tabla}")
            cols_sql = [f"{col} {tipo}" for col, tipo in ESQUEMA_COLUMNAS.items()]
            sql_create = text(f"CREATE TABLE {nombre_tabla} ({', '.join(cols_sql)});")
            with self.engine.begin() as conn:
                conn.execute(sql_create)
            return

        columnas_existentes = [col['name'] for col in inspector.get_columns(nombre_tabla)]
        columnas_faltantes = [col for col in ESQUEMA_COLUMNAS.keys() if col not in columnas_existentes]

        if columnas_faltantes:
            print(f"   🔧 Actualizando esquema de {nombre_tabla}. Faltaban: {columnas_faltantes}")
            with self.engine.begin() as conn:
                for col in columnas_faltantes:
                    tipo = ESQUEMA_COLUMNAS[col]
                    if col != 'id': 
                        conn.execute(text(f"ALTER TABLE {nombre_tabla} ADD COLUMN {col} {tipo}"))

    def sincronizar_secuencia(self, nombre_tabla):
        try:
            with self.engine.begin() as conn:
                sql_sync = text(f"""
                    SELECT setval(
                        pg_get_serial_sequence('{nombre_tabla}', 'id'), 
                        COALESCE((SELECT MAX(id) FROM {nombre_tabla}), 0) + 1, 
                        false
                    );
                """)
                conn.execute(sql_sync)
        except Exception as e:
            pass

    def preparar_dataframe(self, df):
        df.columns = [c.strip() for c in df.columns]
        
        col_fecha = next((c for c in ['Fecha_DT', 'Fecha', 'fecha'] if c in df.columns), None)
        if not col_fecha: raise ValueError("Columna fecha no encontrada")

        mapeo = {
            col_fecha: 'fecha', 'Año': 'ano', 'Mes': 'mes', 'Semana': 'semana',
            'Referencia Bancaria': 'referencia_bancaria', 'Descripcion': 'descripcion',
            'Monto REF': 'monto_ref', 'Saldo REF': 'saldo_ref', 'Moneda REF': 'moneda_ref',
            'Tasa de Cambio': 'tasa_cambio', 'Monto USD': 'monto_usd', 
            'Concepto EY': 'concepto_ey', 'Proveedor/Cliente': 'proveedor_cliente',
            'Tipo de Operacion': 'tipo_operacion', 'Banco': 'banco'
        }
        
        df = df.rename(columns=mapeo)
        cols_validas = [c for c in df.columns if c in ESQUEMA_COLUMNAS and c != 'id']
        df = df[cols_validas].copy()

        # Limpieza
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=True)
        df = df.dropna(subset=['fecha'])

        cols_numericas = ['monto_ref', 'saldo_ref', 'tasa_cambio', 'monto_usd']
        for col in cols_numericas:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)

        if 'moneda_ref' in df.columns:
            df['moneda_ref'] = df['moneda_ref'].replace({'USD': 'US'})
        
        df['fecha_carga'] = datetime.now()
        df['es_saldo_final'] = False
        df['saldo_final_calculado'] = 0.0
        
        if 'ano' not in df.columns:
            df['ano'] = df['fecha'].dt.year

        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        return df

    def cargar_archivo(self, ruta_archivo):
        try:
            nombre_tabla = self.determinar_nombre_tabla(ruta_archivo)
            
            # 1. Asegurar Estructura
            self.asegurar_tabla_y_columnas(nombre_tabla)

            # 2. Sincronizar Secuencia de IDs
            self.sincronizar_secuencia(nombre_tabla)

            # 3. Leer y preparar
            try: df = pd.read_excel(ruta_archivo)
            except: df = pd.read_csv(ruta_archivo, sep=None, engine='python')

            if df.empty: return False

            df_final = self.preparar_dataframe(df)
            if df_final.empty: return False

            # 4. Limpiar datos del MISMO mes/año (Upsert manual)
            meses = df_final['fecha'].dt.month.unique()
            anos = df_final['fecha'].dt.year.unique()
            
            if len(meses) > 0:
                filtro_mes = ",".join([str(int(m)) for m in meses if pd.notna(m)])
                filtro_ano = ",".join([str(int(a)) for a in anos if pd.notna(a)])
                
                with self.engine.begin() as conn:
                    sql_del = text(f"""
                        DELETE FROM {nombre_tabla} 
                        WHERE EXTRACT(MONTH FROM fecha) IN ({filtro_mes})
                        AND EXTRACT(YEAR FROM fecha) IN ({filtro_ano})
                    """)
                    conn.execute(sql_del)
                    print(f"   🧹 Limpieza previa: Mes {filtro_mes}/{filtro_ano}")

            # 5. Insertar
            df_final.to_sql(name=nombre_tabla, con=self.engine, if_exists='append', index=False, chunksize=1000)
            print(f"   📥 Carga exitosa: {len(df_final)} registros -> {nombre_tabla}")
            return True

        except Exception as e:
            err_msg = str(e)
            if "UniqueViolation" in err_msg:
                print(f"   ❌ Error ID Duplicado: La secuencia no se pudo sincronizar automáticamente. Detalles: {err_msg.split('DETAIL')[0]}")
            else:
                print(f"   ❌ Error en {os.path.basename(ruta_archivo)}: {err_msg.split('[SQL')[0]}")
            return False

    def procesar_carpeta(self, carpeta_origen):
        print(f"\n📂 Iniciando carga desde: {carpeta_origen}")
        archivos = glob.glob(f"{carpeta_origen}/**/*.xlsx", recursive=True)
        archivos = [f for f in archivos if "~$" not in f]
        
        for archivo in sorted(archivos):
            print(f"\n📄 Procesando: {os.path.basename(archivo)}")
            self.cargar_archivo(archivo)

if __name__ == "__main__":
    CARPETA_DATOS = "estados_cuenta_processed"
    loader = PostgreSQLBancoLoader(db_config)
    loader.procesar_carpeta(CARPETA_DATOS)