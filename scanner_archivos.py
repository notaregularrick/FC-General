import os
import re
from dotenv import load_dotenv

class ScannerAutomatico:
    def __init__(self, subcarpeta_datos):
        load_dotenv()
        self.ruta_base = os.getenv("dirIn")
        self.carpeta_datos = os.path.join(self.ruta_base, subcarpeta_datos)
        
        if not os.path.exists(self.carpeta_datos):
            raise FileNotFoundError(f"❌ La carpeta no existe: {self.carpeta_datos}")

    def obtener_ruta_tasas(self, nombre_archivo="TC-2025.xlsx"):
        """Busca el archivo de tasas en la raíz."""
        return os.path.join(self.ruta_base, nombre_archivo)

    def escanear_meses(self, criterio_nombre="BANCAMIGA"):
        """
        Escanea la carpeta buscando archivos que contengan 'criterio_nombre'.
        
        Args:
            criterio_nombre (str): El nombre del banco a buscar (ej: "BANCAMIGA", "BANESCO", "MERCANTIL").
            
        Returns:
            dict: { mes: {'bs': ruta, 'usd': ruta, 'nombre_mes': str} }
        """
        print(f"🕵️  Escaneando carpeta: {self.carpeta_datos}")
        print(f"🔎  Filtro aplicado: Archivos que contengan '{criterio_nombre}'")
        
        # 1. Obtenemos TODOS los excels (CORREGIDO CON TUPLA)
        try:
            todos_archivos = [f for f in os.listdir(self.carpeta_datos) if f.endswith((".xlsx", ".xls"))]
        except FileNotFoundError:
            print("❌ Error: No se pudo leer el directorio.")
            return {}

        # 2. Filtramos por el nombre del banco (BANCAMIGA, BANESCO, etc.)
        archivos_filtrados = [f for f in todos_archivos if criterio_nombre.upper() in f.upper()]
        
        grupos_mes = {}

        # Regex para buscar fechas tipo "01-11", "30/11", "11-2025" en el nombre
        # Captura: (Dia)[Separador](Mes)
        patron_fecha = re.compile(r"(\d{1,2})[-/](\d{1,2})")

        for archivo in archivos_filtrados:
            ruta_completa = os.path.join(self.carpeta_datos, archivo)
            nombre_upper = archivo.upper()
            
            # 3. Detectar Mes (Buscamos la primera fecha en el nombre)
            match = patron_fecha.search(archivo)
            if match:
                # El segundo grupo suele ser el mes en formatos dd-mm
                mes = int(match.group(2)) 
                
                # Validación extra: Si el mes detectado es > 12, quizás el regex capturó el día como mes
                # (Ej: capturó el 20 de "20-10"). Invertimos lógica básica si es necesario.
                if mes > 12:
                    mes = int(match.group(1))
            else:
                print(f"⚠️  No se detectó fecha en: {archivo}. Se saltará este archivo.")
                continue

            if mes > 12 or mes < 1:
                print(f"⚠️  Mes inválido ({mes}) detectado en: {archivo}. Saltando.")
                continue

            # 4. Inicializar estructura del mes
            if mes not in grupos_mes:
                grupos_mes[mes] = {
                    'bs': None, 
                    'usd': None, 
                    'nombre_mes': self._nombre_mes(mes)
                }

            # 5. Clasificar BS vs USD
            if "USD" in nombre_upper or "DOLARES" in nombre_upper:
                grupos_mes[mes]['usd'] = ruta_completa
            else:
                grupos_mes[mes]['bs'] = ruta_completa

        # Ordenar cronológicamente
        grupos_ordenados = dict(sorted(grupos_mes.items()))
        
        print(f"✅ Se encontraron archivos de {criterio_nombre} para {len(grupos_ordenados)} meses distintos.")
        return grupos_ordenados

    def _nombre_mes(self, numero_mes):
        meses = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                 7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
        return meses.get(numero_mes, f"Mes-{numero_mes}")
    
    def listar_archivos_simples(self, criterio_nombre):
        """
        Devuelve una LISTA PLANA de todas las rutas que coincidan con el nombre.
        Útil cuando hay múltiples archivos para un mismo mes.
        """
        # (CORREGIDO CON TUPLA PARA INCLUIR .xls AQUÍ TAMBIÉN)
        archivos = [f for f in os.listdir(self.carpeta_datos) if f.endswith((".xlsx", ".xls", ".csv"))]
        rutas_encontradas = []

        for archivo in archivos:
            if criterio_nombre.upper() in archivo.upper():
                rutas_encontradas.append(os.path.join(self.carpeta_datos, archivo))
        
        print(f"📦 Se encontraron {len(rutas_encontradas)} archivos crudos para '{criterio_nombre}'.")
        return rutas_encontradas