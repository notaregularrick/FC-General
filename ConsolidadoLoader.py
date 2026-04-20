import sys
import os
import importlib
from dotenv import load_dotenv

# Importamos nuestras herramientas compartidas
from scanner_archivos import ScannerAutomatico
from utils_comunes import GestorTasas

# Lista de archivos normalizadores a ejecutar
# Lista de módulos (Nombres de los archivos sin .py)
MODULOS_NORM = [
    "Bancamiga_Norm",
    "Banesco99_Norm",
    "BanescoPlanta_Norm",
    "BanescoVerde_Norm",
    "BanescoPanam_Norm",
    "Banplus_Norm",
    "BDVNorm",
    "BNC_Norm",
    "Mercantil_Norm",
    "MercantilPan_Norm",
    "Provincial_Norm",
    "bvc_norm",
    "caja_norm",
    "BNC6550_Norm"    
]

def load_balance():
    load_dotenv()
    
    print("\n" + "="*60)
    print("🏦 INICIANDO CARGA DE BALANCE CONSOLIDADO (MODO INTEGRADO)")
    print("="*60)

    # ---------------------------------------------------------
    # 1. INICIALIZACIÓN GLOBAL (SE HACE UNA SOLA VEZ) 🚀
    # ---------------------------------------------------------
    try:
        # Inicializar Scanner
        # Nota: La subcarpeta se puede cambiar dinámicamente dentro de cada módulo si es necesario
        scanner_global = ScannerAutomatico(subcarpeta_datos="Ed_cuenta_Yolo_mar")
        
        # Cargar Tasas (Singleton - Lectura pesada solo ocurre aquí)
        ruta_tasas = scanner_global.obtener_ruta_tasas()
        gestor_tasas_global = GestorTasas(ruta_tasas) 
        
        print("✅ Herramientas globales inicializadas correctamente.")
        
    except Exception as e:
        print(f"⛔ Error fatal inicializando herramientas: {e}")
        return False

    exitosos = 0
    fallidos = 0

    # ---------------------------------------------------------
    # 2. EJECUCIÓN SECUENCIAL EN MEMORIA
    # ---------------------------------------------------------
    for nombre_modulo in MODULOS_NORM:
        print(f"\n{'='*60}")
        print(f"🔄 Cargando módulo: {nombre_modulo}")
        
        try:
            # Importación dinámica del script
            modulo = importlib.import_module(nombre_modulo)
            
            # Verificamos si tiene la función estándar
            if hasattr(modulo, 'ejecutar_normalizacion'):
                
                # 🔥 EJECUCIÓN COMPARTIDA 🔥
                # Le pasamos las instancias ya creadas
                resultado = modulo.ejecutar_normalizacion(scanner_global, gestor_tasas_global)
                
                if resultado:
                    print(f"✅ {nombre_modulo} finalizó con éxito.")
                    exitosos += 1
                else:
                    print(f"⚠️ {nombre_modulo} finalizó pero reportó sin datos/warning.")
                    # Lo contamos como éxito parcial o fallo según tu criterio. 
                    # Si no rompió, sumamos exitosos.
                    exitosos += 1 
            else:
                print(f"❌ El módulo {nombre_modulo} no tiene la función 'ejecutar_normalizacion'.")
                fallidos += 1
                
        except Exception as e:
            print(f"❌ Error ejecutando {nombre_modulo}: {e}")
            import traceback
            traceback.print_exc()
            fallidos += 1

    # ---------------------------------------------------------
    # 3. RESUMEN
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print("📊 RESUMEN DE EJECUCIÓN")
    print("="*60)
    print(f"✅ Módulos procesados: {exitosos}/{len(MODULOS_NORM)}")
    print(f"❌ Fallos críticos:    {fallidos}")
    
    return fallidos == 0


if __name__ == "__main__":
    load_balance()
