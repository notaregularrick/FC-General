import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

class ConsolidadorBancos:
    def __init__(self, ruta_base, carpeta_salida="reportes_consolidados"):
        self.ruta_base = ruta_base
        self.carpeta_salida = carpeta_salida
        self.crear_carpetas()
    
    def crear_carpetas(self):
        """Crea la estructura de carpetas si no existe"""
        carpetas = [
            self.carpeta_salida,
            f"{self.carpeta_salida}/consolidados_mensuales",
            f"{self.carpeta_salida}/backups"
        ]
        
        for carpeta in carpetas:
            os.makedirs(carpeta, exist_ok=True)
            print(f"📁 Carpeta creada/verificada: {carpeta}")
    
    def buscar_archivos_normalizados(self, bancos):
        """Busca todos los archivos normalizados en las carpetas de bancos especificados"""
        print("🔍 Buscando archivos normalizados...")
        
        archivos_por_banco = {}
        
        for banco in bancos:
            ruta_banco = os.path.join(self.ruta_base, banco)
            
            if not os.path.exists(ruta_banco):
                print(f"❌ No se encontró la carpeta para {banco}")
                continue
            
            # Buscar archivos Excel en la carpeta del banco
            archivos_excel = []
            for archivo in os.listdir(ruta_banco):
                if archivo.endswith('.xlsx'):
                    archivos_excel.append(os.path.join(ruta_banco, archivo))
            
            archivos_por_banco[banco] = archivos_excel
            print(f"📊 {banco}: {len(archivos_excel)} archivos encontrados")
            
            # Mostrar nombres de archivos
            for archivo in archivos_excel:
                print(f"   📄 {os.path.basename(archivo)}")
        
        return archivos_por_banco
    
    def cargar_archivo_banco(self, ruta_archivo, banco):
        """Carga un archivo normalizado de un banco específico"""
        try:
            print(f"🔄 Cargando archivo: {os.path.basename(ruta_archivo)}")
            df = pd.read_excel(ruta_archivo)
            
            # Verificar que tenga las columnas básicas
            columnas_requeridas = ['Fecha', 'Descripcion', 'Monto REF', 'Saldo REF', 'Moneda REF', 'Banco']
            if not all(col in df.columns for col in columnas_requeridas):
                print(f"⚠️  El archivo no tiene la estructura esperada: {os.path.basename(ruta_archivo)}")
                return pd.DataFrame()
            
            # Agregar información de origen
            df['Archivo_Origen'] = os.path.basename(ruta_archivo)
            df['Fecha_Carga'] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            
            print(f"✅ Cargado: {len(df)} registros")
            return df
            
        except Exception as e:
            print(f"❌ Error cargando {ruta_archivo}: {e}")
            return pd.DataFrame()
    
    def obtener_rango_fechas_consolidado(self, df):
        """Obtiene el rango de fechas del DataFrame consolidado en formato dd-mm-aa"""
        if df.empty or 'Fecha' not in df.columns:
            return ""
        
        try:
            # Convertir fechas a datetime
            df['Fecha_dt'] = pd.to_datetime(df['Fecha'], format='%d-%m-%Y', errors='coerce')
            
            # Eliminar valores NaT
            fechas_validas = df['Fecha_dt'].dropna()
            
            if len(fechas_validas) == 0:
                return ""
            
            fecha_min = fechas_validas.min()
            fecha_max = fechas_validas.max()
            
            # Formatear como dd-mm-aa (año con 2 dígitos)
            fecha_min_str = fecha_min.strftime('%d-%m-%y')
            fecha_max_str = fecha_max.strftime('%d-%m-%y')
            
            return f"{fecha_min_str} al {fecha_max_str}"
            
        except Exception as e:
            print(f"⚠️  Error obteniendo rango de fechas: {e}")
            return ""
    
    def consolidar_bancos(self, bancos):
        """Consolida todos los archivos de los bancos especificados"""
        print(f"\n🎯 INICIANDO CONSOLIDACIÓN DE BANCOS")
        print(f"📋 Bancos a consolidar: {', '.join(bancos)}")
        
        # Buscar archivos
        archivos_por_banco = self.buscar_archivos_normalizados(bancos)
        
        if not archivos_por_banco:
            print("❌ No se encontraron archivos para consolidar")
            return pd.DataFrame()
        
        # Consolidar todos los datos
        todos_los_datos = []
        estadisticas = {}
        
        for banco, archivos in archivos_por_banco.items():
            print(f"\n{'='*50}")
            print(f"🔄 Procesando {banco}...")
            
            datos_banco = []
            for archivo in archivos:
                df_archivo = self.cargar_archivo_banco(archivo, banco)
                if not df_archivo.empty:
                    datos_banco.append(df_archivo)
            
            if datos_banco:
                # Consolidar datos del banco
                df_banco = pd.concat(datos_banco, ignore_index=True)
                todos_los_datos.append(df_banco)
                
                # Estadísticas del banco
                estadisticas[banco] = {
                    'archivos': len(datos_banco),
                    'registros': len(df_banco),
                    'fecha_min': df_banco['Fecha'].min() if 'Fecha' in df_banco.columns else 'N/A',
                    'fecha_max': df_banco['Fecha'].max() if 'Fecha' in df_banco.columns else 'N/A',
                    'total_usd': df_banco['Monto USD'].sum() if 'Monto USD' in df_banco.columns else 0
                }
                
                print(f"✅ {banco}: {len(df_banco)} registros consolidados")
            else:
                print(f"⚠️  No se pudieron cargar archivos para {banco}")
        
        if not todos_los_datos:
            print("❌ No hay datos para consolidar")
            return pd.DataFrame()
        
        # Consolidar todos los bancos
        print(f"\n🔄 Consolidando todos los bancos...")
        df_consolidado = pd.concat(todos_los_datos, ignore_index=True)
        
        # Ordenar por fecha
        if 'Fecha' in df_consolidado.columns:
            df_consolidado['Fecha_dt'] = pd.to_datetime(df_consolidado['Fecha'], format='%d-%m-%Y', errors='coerce')
            df_consolidado = df_consolidado.sort_values('Fecha_dt')
            df_consolidado = df_consolidado.drop('Fecha_dt', axis=1)
        
        # Mostrar estadísticas finales
        self.mostrar_estadisticas_consolidacion(estadisticas, df_consolidado)
        
        return df_consolidado
    
    def mostrar_estadisticas_consolidacion(self, estadisticas, df_consolidado):
        """Muestra estadísticas detalladas de la consolidación"""
        print(f"\n📊 ESTADÍSTICAS DE CONSOLIDACIÓN")
        print(f"{'='*50}")
        
        total_registros = 0
        total_archivos = 0
        
        for banco, stats in estadisticas.items():
            print(f"🏦 {banco.upper()}:")
            print(f"   📄 Archivos procesados: {stats['archivos']}")
            print(f"   📊 Registros: {stats['registros']:,}")
            print(f"   📅 Rango fechas: {stats['fecha_min']} a {stats['fecha_max']}")
            if stats['total_usd'] != 0:
                print(f"   💰 Total USD: {stats['total_usd']:,.2f}")
            print()
            
            total_registros += stats['registros']
            total_archivos += stats['archivos']
        
        print(f"📈 RESUMEN GENERAL:")
        print(f"   🏦 Bancos consolidados: {len(estadisticas)}")
        print(f"   📄 Total archivos: {total_archivos}")
        print(f"   📊 Total registros: {total_registros:,}")
        
        if not df_consolidado.empty:
            # Estadísticas del DataFrame consolidado
            if 'Fecha' in df_consolidado.columns:
                fechas_unicas = df_consolidado['Fecha'].nunique()
                fecha_min = df_consolidado['Fecha'].min()
                fecha_max = df_consolidado['Fecha'].max()
                print(f"   📅 Fechas únicas: {fechas_unicas}")
                print(f"   🗓️  Rango completo: {fecha_min} a {fecha_max}")
            
            if 'Banco' in df_consolidado.columns:
                bancos_unicos = df_consolidado['Banco'].nunique()
                print(f"   🏛️  Bancos en consolidado: {bancos_unicos}")
            
            if 'Tipo de Operacion' in df_consolidado.columns:
                creditos = len(df_consolidado[df_consolidado['Tipo de Operacion'] == 'CREDITO'])
                debitos = len(df_consolidado[df_consolidado['Tipo de Operacion'] == 'DEBITO'])
                print(f"   💳 Créditos: {creditos:,}")
                print(f"   💸 Débitos: {debitos:,}")
            
            if 'Monto USD' in df_consolidado.columns:
                total_usd = df_consolidado['Monto USD'].sum()
                print(f"   💰 Monto total USD: {total_usd:,.2f}")
    
    def guardar_reporte_consolidado(self, df, nombre_archivo=None):
        """Guarda el reporte consolidado"""
        if df.empty:
            print("❌ No hay datos para guardar")
            return False
        
        # Obtener rango de fechas para el nombre del archivo
        rango_fechas = self.obtener_rango_fechas_consolidado(df)
        
        # Crear nombre de archivo con rango de fechas
        if nombre_archivo is None:
            if rango_fechas:
                nombre_archivo = f"Reporte_Consolidado_Bancos_{rango_fechas}.xlsx"
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nombre_archivo = f"Reporte_Consolidado_Bancos_{timestamp}.xlsx"
        
        ruta_completa = os.path.join(self.carpeta_salida, nombre_archivo)
        
        try:
            # Guardar el DataFrame consolidado
            df.to_excel(ruta_completa, index=False)
            
            print(f"\n💾 REPORTE GUARDADO EXITOSAMENTE")
            print(f"📁 Ubicación: {ruta_completa}")
            print(f"📊 Total registros: {len(df):,}")
            if rango_fechas:
                print(f"📅 Rango de fechas: {rango_fechas}")
            
            # Crear también un archivo de resumen
            self.crear_archivo_resumen(df, nombre_archivo, rango_fechas)
            
            return True
            
        except Exception as e:
            print(f"❌ Error guardando reporte consolidado: {e}")
            return False
    
    def crear_archivo_resumen(self, df, nombre_archivo_base, rango_fechas=""):
        """Crea un archivo de resumen con estadísticas"""
        try:
            # Generar estadísticas por banco
            if 'Banco' in df.columns:
                resumen_banco = df.groupby('Banco').agg({
                    'Monto USD': ['count', 'sum', 'mean'],
                    'Tipo de Operacion': lambda x: (x == 'CREDITO').sum()
                }).round(2)
                
                resumen_banco.columns = ['Total_Transacciones', 'Suma_USD', 'Promedio_USD', 'Total_Creditos']
                resumen_banco['Total_Debitos'] = resumen_banco['Total_Transacciones'] - resumen_banco['Total_Creditos']
                
                # Guardar resumen con rango de fechas
                if rango_fechas:
                    nombre_resumen = nombre_archivo_base.replace('.xlsx', f'_RESUMEN_{rango_fechas}.xlsx')
                else:
                    nombre_resumen = nombre_archivo_base.replace('.xlsx', '_RESUMEN.xlsx')
                
                ruta_resumen = os.path.join(self.carpeta_salida, nombre_resumen)
                resumen_banco.to_excel(ruta_resumen)
                
                print(f"📋 Archivo de resumen: {ruta_resumen}")
                
        except Exception as e:
            print(f"⚠️  No se pudo crear archivo de resumen: {e}")

# USO DEL CONSOLIDADOR
if __name__ == "__main__":
    load_dotenv()
    # Ruta base donde están las carpetas de bancos normalizados
    ruta_base = os.getenv("dir")
    
    # Bancos a consolidar (nombres de las carpetas)
    bancos_a_consolidar = ["bancamiga", "banesco99", "banplus", "provincial", "mercantil","bnc","bdv","banesco_planta","mercantil_panama"]
    
    print("🚀 INICIANDO CONSOLIDADOR DE BANCOS")
    print(f"📁 Ruta base: {ruta_base}")
    print(f"🏦 Bancos a consolidar: {', '.join(bancos_a_consolidar)}")
    
    # Verificar que existe la ruta base
    if not os.path.exists(ruta_base):
        print(f"❌ No se encuentra la ruta base: {ruta_base}")
        exit()
    
    # Inicializar consolidador
    consolidador = ConsolidadorBancos(ruta_base)
    
    # Consolidar bancos
    df_consolidado = consolidador.consolidar_bancos(bancos_a_consolidar)
    
    # Guardar reporte consolidado
    if df_consolidado is not None and not df_consolidado.empty:
        success = consolidador.guardar_reporte_consolidado(df_consolidado)
        if success:
            print(f"\n🎉 CONSOLIDACIÓN COMPLETADA EXITOSAMENTE!")
            print(f"💡 El reporte contiene todos los datos de:")
            for banco in bancos_a_consolidar:
                print(f"   - {banco}")
        else:
            print(f"\n❌ Error al guardar el reporte consolidado")
    else:
        print("❌ No se pudo generar el reporte consolidado")