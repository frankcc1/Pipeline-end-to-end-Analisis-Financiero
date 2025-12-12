import datetime
import time
import os
import logging
import treasury
import sbs_soberana
import sbs_bcrp
import curva_dolares
import sura_fondo

# ==========================================
# CONFIGURACIÓN DE LOGS
# ==========================================
def setup_logger():
    log_folder = "Logs"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Log acumulativo
    log_filename = os.path.join(log_folder, "Extraccion_History.log")

    logger = logging.getLogger('Financial_ETL')
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
# ORQUESTADOR PRINCIPAL
# ==========================================
def main():
    log = setup_logger()
    start_time = datetime.datetime.now()
    
    log.info("╔════════════════════════════════════════════════════════╗")
    log.info("║   INICIO DEL PROCESO DE EXTRACCIÓN (RAW FILES)         ║")
    log.info("╚════════════════════════════════════════════════════════╝")

    # Módulos a ejecutar
    modules_to_run = [
        (treasury, "Curva Treasury (USA)"),
        (sbs_soberana, "Curva Soberana Soles (SBS)"),
        (sbs_bcrp, "Curvas CD BCRP (SBS)"),
        (curva_dolares, "Curva Dólares CP (SBS)"),
        (sura_fondo, "Fondo Sura Ultra Cash (SMV)")
    ]

    errors_count = 0

    # --- EJECUCIÓN ---
    for module, name in modules_to_run:
        log.info(f"--- Ejecutando módulo: {name} ---")
        
        try:
            # Ejecutamos la extracción
            # Nota: Ahora esperamos que devuelvan True/False y un Mensaje.
            # Ignoramos el segundo valor (df) porque estamos en modo 'Solo Archivos'.
            success, _, msg = module.run_extraction()

            if success:
                log.info(f"✔ ÉXITO | {msg}")
            else:
                log.error(f"✖ FALLO | {name}: {msg}")
                errors_count += 1
        
        except AttributeError:
            log.critical(f"☠ ERROR CÓDIGO | {name} no tiene función 'run_extraction'.")
            errors_count += 1
        except Exception as e:
            log.critical(f"☠ CRASH | Error en {name}: {str(e)}")
            errors_count += 1
        
        time.sleep(1)

    # --- REPORTE FINAL ---
    duration = datetime.datetime.now() - start_time
    log.info("----------------------------------------------------------")
    log.info(f"FIN DEL PROCESO. | Duración: {duration} | Errores: {errors_count}")
    log.info("Verificar carpeta 'Raw_Files' para los archivos descargados.")

if __name__ == "__main__":
    main()