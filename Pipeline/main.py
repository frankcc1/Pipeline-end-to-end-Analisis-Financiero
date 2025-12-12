import os
import sys
import logging
import datetime
import time

# ==========================================
# 1. CONFIGURACIÓN DE RUTAS (PATH)
# ==========================================
# Calculamos la raíz del proyecto de forma absoluta
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTRACTION_DIR = os.path.join(BASE_DIR, 'Data_Extraction')
PREPARATION_DIR = os.path.join(BASE_DIR, 'Data_Preparation')

# Agregamos las rutas al sistema para los imports
sys.path.append(EXTRACTION_DIR)
sys.path.append(PREPARATION_DIR)

try:
    # Módulos de Extracción
    import treasury
    import sbs_soberana
    import sbs_bcrp
    import curva_dolares
    import sura_fondo 
    
    # Módulo de Transformación
    from Transformacion import TransformationManager
except ImportError as e:
    print(f"❌ Error crítico de importación: {e}")
    sys.exit(1)

# ==========================================
# 2. CONFIGURACIÓN DE LOGS
# ==========================================
def setup_pipeline_logger():
    log_folder = os.path.join(BASE_DIR, 'Pipeline', 'Logs')
    os.makedirs(log_folder, exist_ok=True)
    
    log_filename = os.path.join(log_folder, f"Pipeline_Run_{datetime.datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger('Master_Pipeline')
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# ==========================================
# 3. LÓGICA PRINCIPAL (ETL)
# ==========================================
def run_pipeline():
    log = setup_pipeline_logger()
    start_time = datetime.datetime.now()
    
    # Guardamos la ubicación original (Pipeline folder)
    original_cwd = os.getcwd()
    
    log.info("╔════════════════════════════════════════════════════════╗")
    log.info("║       INICIO DEL PIPELINE FINANCIERO (ETL)             ║")
    log.info("╚════════════════════════════════════════════════════════╝")

    # --- FASE 1: EXTRACCIÓN (Bronze Layer) ---
    log.info(">>> FASE 1: EXTRACCIÓN DE DATOS (RAW)")
    
    modules_to_run = [
        (treasury, "Curva Treasury (USA)"),
        (sbs_soberana, "Curva Soberana Soles (SBS)"),
        (sbs_bcrp, "Curvas CD BCRP (SBS)"),
        (curva_dolares, "Curva Dólares CP (SBS)"),
    ]

    extraction_errors = 0

    # === TRUCO SENIOR: CAMBIO DE CONTEXTO ===
    # Nos movemos a la carpeta Data_Extraction para que los archivos se guarden ahí
    try:
        log.info(f"📍 Cambiando directorio de trabajo a: {EXTRACTION_DIR}")
        os.chdir(EXTRACTION_DIR) # <--- AQUÍ ESTÁ LA MAGIA
    except Exception as e:
        log.critical(f"No se pudo acceder al directorio de extracción: {e}")
        return

    # Ejecutamos la extracción (ahora estando parados en Data_Extraction)
    for module, name in modules_to_run:
        log.info(f"   ► Ejecutando extracción: {name}")
        try:
            success, _, msg = module.run_extraction()
            
            if success:
                log.info(f"     ✔ ÉXITO: {msg}")
            else:
                log.error(f"     ✖ FALLO: {msg}")
                extraction_errors += 1
        except Exception as e:
            log.critical(f"     ☠ CRASH en {name}: {str(e)}")
            extraction_errors += 1
        time.sleep(1)

    # Regresamos al directorio original por seguridad
    os.chdir(original_cwd) 
    log.info(f"📍 Regresando directorio de trabajo a: {original_cwd}")

    if extraction_errors > 0:
        log.warning(f"⚠ FASE 1 con errores ({extraction_errors}). Revisar Logs.")
    else:
        log.info("✔ FASE 1 COMPLETADA SIN ERRORES.")

    # --- FASE 2: TRANSFORMACIÓN (Silver/Gold Layer) ---
    log.info("----------------------------------------------------------")
    log.info(">>> FASE 2: TRANSFORMACIÓN Y LIMPIEZA (PROCESS)")
    
    # La transformación usa rutas absolutas (BASE_DIR), así que no necesita os.chdir
    try:
        transformer = TransformationManager()
        transform_errors = 0
        
        # Lista de tareas de transformación
        tasks = [
            (transformer.process_treasury, "Treasury"),
            (transformer.process_sbs_soberana, "SBS Soberana"),
            (transformer.process_sbs_bcrp, "SBS BCRP"),
            (transformer.process_curva_dolares, "Curva Dólares")
        ]

        for func, task_name in tasks:
            if func(): # Ahora esperamos el True/False que agregamos antes
                log.info(f"     ✔ {task_name} Processed & Saved")
            else:
                log.error(f"     ✖ {task_name} FAILED")
                transform_errors += 1
        
        if transform_errors == 0:
            log.info("✔ FASE 2 COMPLETADA EXITOSAMENTE.")
        else:
            log.warning(f"⚠ FASE 2 CON ERRORES: {transform_errors}")

    except Exception as e:
        log.critical(f"☠ CRASH CRÍTICO EN TRANSFORMACIÓN: {str(e)}")

    # --- CIERRE ---
    duration = datetime.datetime.now() - start_time
    log.info("==========================================================")
    log.info(f"   FIN DEL PIPELINE | Duración Total: {duration}")
    log.info("==========================================================")

if __name__ == "__main__":
    run_pipeline()