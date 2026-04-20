import pandas as pd
from datetime import datetime
import os

# =============================================================================
# 📅 LÓGICA DE TIEMPO (SEMANAS)
# =============================================================================
def obtener_semana_corte_viernes(fecha):
    """
    Determina la semana del mes para una fecha dada.
    
    NUEVA LÓGICA (Por número de día):
    - Semana 1: Días 1 al 7
    - Semana 2: Días 8 al 14
    - Semana 3: Días 15 al 21
    - Semana 4: Días 22 en adelante (incluye 28, 29, 30 y 31)
    
    * Nota: Se mantiene el nombre 'obtener_semana_corte_viernes' por 
      compatibilidad con los normalizadores existentes.
    """
    if pd.isna(fecha):
        return "Fecha Inválida"

    # Asegurar que sea datetime
    if not isinstance(fecha, datetime):
        try:
            fecha = pd.to_datetime(fecha)
        except:
            return "Fecha Inválida"

    month = fecha.month
    day = fecha.day

    nombres_meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    nombre_mes = nombres_meses.get(month, "Desconocido")

    # Lógica de asignación por rango de días
    if day <= 7:
        num_semana = 1
    elif day <= 14:
        num_semana = 2
    elif day <= 21:
        num_semana = 3
    else:
        num_semana = 4

    return f"Semana {num_semana} - {nombre_mes}"

# =============================================================================
# 💱 LÓGICA DE TASAS DE CAMBIO (Singleton)
# =============================================================================
class GestorTasas:
    _instancia = None
    
    def __new__(cls, ruta_archivo=None):
        if cls._instancia is None:
            print("✨ Creando la INSTANCIA ÚNICA del Gestor de Tasas...")
            cls._instancia = super(GestorTasas, cls).__new__(cls)
            cls._instancia.df_tasas = pd.DataFrame()
            if ruta_archivo:
                cls._instancia.cargar_tasas(ruta_archivo)
        return cls._instancia

    def cargar_tasas(self, ruta_archivo):
        if not os.path.exists(ruta_archivo):
            print(f"⚠️  Archivo de tasas no encontrado en: {ruta_archivo}")
            return
            
        try:
            print(f"📊 [I/O] Leyendo Excel de tasas desde disco: {ruta_archivo}")
            self.df_tasas = pd.read_excel(ruta_archivo)
            self.df_tasas.columns = ['Fecha', 'Tasa']
            
            # Limpiar y convertir la columna Fecha a datetime
            self.df_tasas['Fecha'] = pd.to_datetime(self.df_tasas['Fecha'], errors='coerce')
            self.df_tasas = self.df_tasas.dropna(subset=['Fecha'])
            
            # Ordenar por fecha para facilitar la búsqueda
            self.df_tasas = self.df_tasas.sort_values('Fecha')

            print(f"✅ Tasas cargadas: {len(self.df_tasas)} registros.")    
        except Exception as e:
            print(f"❌ Error cargando tasas: {e}")

    def obtener_dataframe(self):
        if self.df_tasas.empty:
            print("⚠️ Advertencia: Se pidieron tasas pero el DataFrame está vacío.")
        return self.df_tasas
    
    def obtener_tasa(self, fecha_busqueda):
        """Busca la tasa para una fecha específica (o la más cercana anterior)."""
        try:
            if self.df_tasas.empty: return 1.0 # Fallback por si falla la carga

            # Asegurar que sea datetime
            if not isinstance(fecha_busqueda, datetime):
                fecha_busqueda = pd.to_datetime(fecha_busqueda)

            # 1. Búsqueda Exacta
            fila = self.df_tasas[self.df_tasas['Fecha'] == fecha_busqueda]
            if not fila.empty:
                return float(fila['Tasa'].iloc[0])

            # 2. Búsqueda Aproximada (La última tasa disponible antes de esa fecha)
            anteriores = self.df_tasas[self.df_tasas['Fecha'] < fecha_busqueda]
            if not anteriores.empty:
                return float(anteriores.iloc[-1]['Tasa'])
            
            # 3. Si es más antigua que el primer registro, devolver la más vieja disponible
            return float(self.df_tasas.iloc[0]['Tasa'])
            
        except Exception as e:
            # print(f"Error buscando tasa para {fecha_busqueda}: {e}")
            return 1.0