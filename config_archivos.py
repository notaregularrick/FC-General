import os
from dotenv import load_dotenv

class LocalizadorArchivos:
    def __init__(self, subcarpeta_datos="Ed_cuenta_Yolo_Oct"):
        """
        Inicializa el localizador cargando las variables de entorno.
        
        Args:
            subcarpeta_datos (str): Nombre de la carpeta dentro del directorio base
                                    donde están los excels (ej: "Ed_cuenta_Yolo_Oct").
        """
        load_dotenv()
        self.ruta_base = os.getenv("dirIn")
        self.subcarpeta = subcarpeta_datos
        
        if not self.ruta_base:
            raise ValueError("❌ Error: No se encontró la variable de entorno 'dir' en el archivo .env")

    def obtener_ruta_tasas(self, nombre_archivo="Tasas de cambio 2025.xlsx"):
        """Devuelve la ruta completa del archivo de tasas."""
        ruta = os.path.join(self.ruta_base, nombre_archivo)
        if os.path.exists(ruta):
            return ruta
        else:
            print(f"⚠️ Advertencia: No se encuentra el archivo de tasas en: {ruta}")
            return None

    def construir_rutas_mes(self, nombre_bs, nombre_usd=None):
        """
        Construye las rutas para los archivos de un mes específico.
        
        Args:
            nombre_bs (str): Nombre del archivo en Bolívares.
            nombre_usd (str, opcional): Nombre del archivo en Dólares.
            
        Returns:
            dict: Diccionario con las rutas absolutas y su estado de existencia.
        """
        # Construir ruta base para los archivos del mes
        ruta_carpeta_mes = os.path.join(self.ruta_base, self.subcarpeta)
        
        rutas = {
            "bs": {
                "ruta": os.path.join(ruta_carpeta_mes, nombre_bs),
                "existe": False
            },
            "usd": None
        }

        # Validar BS
        if os.path.exists(rutas["bs"]["ruta"]):
            rutas["bs"]["existe"] = True
        
        # Validar USD (si se proporcionó nombre)
        if nombre_usd:
            ruta_usd = os.path.join(ruta_carpeta_mes, nombre_usd)
            rutas["usd"] = {
                "ruta": ruta_usd,
                "existe": os.path.exists(ruta_usd)
            }
            
        return rutas

# --- Bloque de prueba (para ejecutar este archivo solo) ---
if __name__ == "__main__":
    try:
        # 1. Instanciar
        localizador = LocalizadorArchivos()
        
        # 2. Obtener tasas
        print(f"Tasas: {localizador.obtener_ruta_tasas()}")
        
        # 3. Buscar archivos de Noviembre (Ejemplo extraído de tu código original)
        archivos_nov = localizador.construir_rutas_mes(
            nombre_bs="BANCAMIGA 01-11 AL 30-11.xlsx",
            nombre_usd="BANCAMIGA USD AL 01-11 AL 30-11.xlsx"
        )
        
        print("\n--- Resultados de Búsqueda ---")
        print(f"Archivo BS encontrado: {archivos_nov['bs']['existe']} -> {archivos_nov['bs']['ruta']}")
        if archivos_nov['usd']:
            print(f"Archivo USD encontrado: {archivos_nov['usd']['existe']} -> {archivos_nov['usd']['ruta']}")
            
    except Exception as e:
        print(e)