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
    FASE 1: EXTRACCIÓN PURA (Raw Data).
    1. Conecta a la SBS.
    2. Selecciona Curva Dólares y Fecha 2025.
    3. Descarga el archivo.
    4. Lo renombra a 'Curva_Dolares_2025.xls' en la carpeta 'Raw_Files'.
    NO procesa datos ni genera DataFrames.
    """
    url = "http://www.sbs.gob.pe/app/pp/CurvaSoberana/Curvas_Consulta_Historica.asp"
    
    # --- 1. CONFIGURACIÓN DE RUTAS ---
    root_dir = os.getcwd()
    raw_dir = os.path.join(root_dir, "Raw_Files")
    
    if not os.path.exists(raw_dir): 
        os.makedirs(raw_dir)

    # Nombre del archivo objetivo
    target_filename = "curva_dolares.xls"
    target_path = os.path.join(raw_dir, target_filename)

    # Limpieza preventiva: Si ya existe el archivo final, lo borramos para asegurar que baje uno nuevo
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
        print(">>> [Dólares] Iniciando navegador para extracción...")
        driver = webdriver.Chrome(options=chrome_opts)
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # --- 3. SELECCIÓN DE PARÁMETROS ---
        # A) Tipo de Curva
        dropdown_curva = wait.until(EC.presence_of_element_located((By.ID, "as_tip_curva")))
        Select(dropdown_curva).select_by_value("CCSDF")
        time.sleep(3) # Esperar recarga

        # B) Fecha Inicio (Enero 2025)
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
            print(f">>> [Dólares] Fecha inicio seleccionada: {start_date_value}")
        else:
            print(">>> [ALERTA] No se detectó 2025, se usará fecha por defecto.")

        time.sleep(1)

        # --- 4. DESCARGA ---
        print(">>> [Dólares] Descargando archivo...")
        btn_consultar = wait.until(EC.element_to_be_clickable((By.ID, "Consultar")))
        btn_consultar.click()

        # --- 5. ESPERA Y RENOMBRADO ---
        downloaded_file = None
        start_time = time.time()
        
        # Esperamos hasta 60 segundos
        while (time.time() - start_time) < 60:
            # Buscamos archivos en la carpeta, ignorando temporales (.crdownload)
            files = [f for f in glob.glob(os.path.join(raw_dir, "*.*")) if not f.endswith('.crdownload')]
            
            # Filtramos para encontrar el archivo recién creado (no el que acabamos de borrar, sino el nuevo)
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
            return False, None, "Timeout: El archivo no se descargó."

        # Renombrar al nombre objetivo
        # Nota: shutil.move funciona como rename si es en el mismo disco
        shutil.move(downloaded_file, target_path)
        
        print(f">>> [Dólares] EXTRACCIÓN COMPLETADA.")
        print(f">>> [Dólares] Archivo guardado en: {target_path}")

        # Retornamos None en el segundo parámetro porque NO hay DataFrame procesado
        return True, None, f"Archivo guardado: {target_filename}"

    except Exception as e:
        return False, None, f"Error Extracción: {e}"
    
    finally:
        if driver:
            driver.quit()

# --- BLOQUE DE PRUEBA ---
if __name__ == "__main__":
    s, _, m = run_extraction()
    print(f"\nEstado: {'EXITO' if s else 'FALLO'}")
    print(f"Mensaje: {m}")