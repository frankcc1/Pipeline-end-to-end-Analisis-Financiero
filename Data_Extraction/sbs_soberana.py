import time
import os
import glob
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

def run_extraction():
    """
    FASE 1: EXTRACCIÓN PURA - SOBERANA SOLES (Raw Data).
    1. Conecta a la URL de Spreads SBS.
    2. Selecciona 'Curva Soberana Soles' (CCPSS) y Fecha Inicio (Enero 2025).
    3. Descarga el archivo.
    4. Lo renombra a 'sbs_soberana.xls' en la carpeta 'Raw_Files'.
    NO procesa datos ni genera DataFrames.
    """
    # URL Específica solicitada
    url = "https://www.sbs.gob.pe/app/pp/Spreads/Spreads_Consulta_Historica.asp"
    
    # --- 1. CONFIGURACIÓN DE RUTAS ---
    root_dir = os.getcwd()
    raw_dir = os.path.join(root_dir, "Raw_Files")
    
    if not os.path.exists(raw_dir): 
        os.makedirs(raw_dir)

    # Nombre del archivo objetivo
    target_filename = "sbs_soberana.xls"
    target_path = os.path.join(raw_dir, target_filename)

    # Limpieza preventiva: Borrar el archivo final si ya existe
    if os.path.exists(target_path):
        os.remove(target_path)

    # --- 2. CONFIGURACIÓN CHROME ---
    chrome_opts = Options()
    chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_argument("--disable-gpu")
    
    prefs = {
        "download.default_directory": raw_dir, # Descarga directa a la carpeta final
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    chrome_opts.add_experimental_option("prefs", prefs)

    driver = None
    try:
        print(">>> [Soberana] Iniciando navegador...")
        driver = webdriver.Chrome(options=chrome_opts)
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # --- 3. SELECCIÓN DE PARÁMETROS ---
        # A) Tipo de Curva: Soberana Soles (CCPSS)
        print(">>> [Soberana] Seleccionando 'CCPSS'...")
        dropdown_curva = wait.until(EC.presence_of_element_located((By.ID, "as_tip_curva")))
        Select(dropdown_curva).select_by_value("CCPSS")
        
        time.sleep(3) # Esperar recarga del postback

        # B) Fecha Inicio (Enero 2025 o la más antigua del año)
        print(">>> [Soberana] Buscando fecha inicio 2025...")
        select_inicio = Select(driver.find_element(By.ID, "as_fec_cons"))
        
        # Buscar la fecha más antigua de 2025
        fechas_2025 = []
        for opt in select_inicio.options:
            val = opt.get_attribute("value")
            if "2025" in val:
                try:
                    dt = datetime.strptime(val, "%d/%m/%Y")
                    fechas_2025.append((dt, val))
                except: pass
        
        if fechas_2025:
            fechas_2025.sort(key=lambda x: x[0]) # Ordenar ascendente
            start_date_value = fechas_2025[0][1]
            select_inicio.select_by_value(start_date_value)
            print(f"    -> Fecha seleccionada: {start_date_value}")
        else:
            print("    -> [ALERTA] No se detectó 2025, se usará fecha por defecto.")

        time.sleep(1)

        # --- 4. DESCARGA ---
        print(">>> [Soberana] Solicitando descarga...")
        btn_consultar = wait.until(EC.element_to_be_clickable((By.ID, "Consultar")))
        btn_consultar.click()

        # --- 5. ESPERA Y RENOMBRADO ---
        downloaded_file = None
        start_time = time.time()
        
        # Esperamos hasta 60 segundos
        while (time.time() - start_time) < 60:
            # Buscamos archivos NO temporales en Raw_Files
            files = [f for f in glob.glob(os.path.join(raw_dir, "*.*")) if not f.endswith('.crdownload')]
            
            if files:
                # Tomamos el más reciente
                latest_file = max(files, key=os.path.getctime)
                
                # Verificamos que sea "fresco" (creado en el último minuto) y tenga peso
                if (time.time() - os.path.getctime(latest_file)) < 60:
                    if os.path.getsize(latest_file) > 0:
                        downloaded_file = latest_file
                        break
            time.sleep(1)

        if not downloaded_file:
            return False, None, "Timeout: El archivo Soberana no se descargó."

        # Renombrar al nombre objetivo
        shutil.move(downloaded_file, target_path)
        
        print(f">>> [Soberana] EXTRACCIÓN COMPLETADA.")
        print(f">>> [Soberana] Archivo guardado en: {target_path}")

        # Retornamos True, None (sin DF)
        return True, None, f"Archivo guardado: {target_filename}"

    except Exception as e:
        return False, None, f"Error Extracción Soberana: {str(e)}"
    
    finally:
        if driver:
            driver.quit()

# Bloque de prueba
if __name__ == "__main__":
    s, _, m = run_extraction()
    print(f"\nEstado: {'EXITO' if s else 'FALLO'}")
    print(f"Mensaje: {m}")