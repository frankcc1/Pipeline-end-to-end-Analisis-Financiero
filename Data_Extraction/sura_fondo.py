import time
import os
import glob
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

def run_extraction():
    """
    FASE 1: EXTRACCIÓN PURA - SURA SMV.
    CORREGIDO: Usa lógica de 'Snapshot' para no robar archivos de otros scripts.
    """
    url = "https://www.smv.gob.pe/SIMV/Frm_EVCP?data=5A959494701B26421F184C081CACF55BFA328E8EBC"
    
    # --- 1. CONFIGURACIÓN DE RUTAS ---
    root_dir = os.getcwd()
    raw_dir = os.path.join(root_dir, "Raw_Files")
    
    if not os.path.exists(raw_dir): 
        os.makedirs(raw_dir)

    target_filename = "sura_fondo.xls"
    target_path = os.path.join(raw_dir, target_filename)

    # Limpieza previa del archivo objetivo (Sura)
    if os.path.exists(target_path):
        os.remove(target_path)

    # --- 2. CONFIGURACIÓN CHROME ---
    chrome_opts = Options()
    chrome_opts.add_argument("--start-maximized")
    chrome_opts.add_argument("--disable-gpu")
    
    prefs = {
        "download.default_directory": raw_dir,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    chrome_opts.add_experimental_option("prefs", prefs)

    driver = None
    try:
        print(">>> [Sura] Iniciando navegador...")
        driver = webdriver.Chrome(options=chrome_opts)
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # --- 3. SELECCIÓN DE EMPRESA (ID: 113223) ---
        print(">>> [Sura] Seleccionando Empresa...")
        cbo_empresa = wait.until(EC.presence_of_element_located((By.ID, "MainContent_cboDenominacionSocial")))
        Select(cbo_empresa).select_by_value("113223")
        time.sleep(4) 

        # --- 4. SELECCIÓN DE FONDO (ID: 0008) ---
        print(">>> [Sura] Seleccionando Fondo...")
        cbo_fondo = wait.until(EC.presence_of_element_located((By.ID, "MainContent_cboFondo")))
        Select(cbo_fondo).select_by_value("0008")
        time.sleep(3) 

       # --- 5. CONFIGURACIÓN DE FECHAS (2022 - 2024) ---
        print(">>> [Sura] Configurando fechas (01/01/2022 - 31/12/2024)...")
        
        # A) Fecha Inicio
        try:
            input_inicio = driver.find_element(By.ID, "txtFechDesde")
        except:
            input_inicio = driver.find_element(By.ID, "MainContent_txtFechDesde")
        
        input_inicio.clear()
        input_inicio.send_keys("01/01/2022")
        
        # B) Fecha Fin
        try:
            input_fin = driver.find_element(By.ID, "txtFechHasta")
        except:
            try:
                input_fin = driver.find_element(By.ID, "MainContent_txtFechHasta")
            except:
                print(">>> [ALERTA] No se encontró el input de Fecha Fin, intentando continuar...")
                input_fin = None

        if input_fin:
            input_fin.clear()
            input_fin.send_keys("31/12/2024")

        # Cerrar calendarios
        driver.find_element(By.TAG_NAME, "body").click()
        time.sleep(1)
        # --- 6. BUSCAR ---
        print(">>> [Sura] Clic en Buscar...")
        driver.find_element(By.ID, "MainContent_btnBuscar").click()
        time.sleep(5)

        # ===================================================================
        # LÓGICA DE SNAPSHOT (LA SOLUCIÓN AL PROBLEMA DE ARCHIVOS PERDIDOS)
        # ===================================================================
        
        # 1. Tomamos una "foto" de los archivos que YA existen antes de descargar
        archivos_antes = set(glob.glob(os.path.join(raw_dir, "*")))
        
        print(">>> [Sura] Clic en icono Excel...")
        btn_excel = wait.until(EC.element_to_be_clickable((By.ID, "MainContent_imgexcel")))
        btn_excel.click()

        # 2. Esperamos a que aparezca un archivo que NO estaba en la foto "archivos_antes"
        print(">>> [Sura] Esperando archivo NUEVO...")
        downloaded_file = None
        start_time = time.time()
        
        while (time.time() - start_time) < 60:
            # Tomamos foto actual
            archivos_ahora = set(glob.glob(os.path.join(raw_dir, "*")))
            
            # Calculamos la diferencia (los nuevos)
            nuevos = archivos_ahora - archivos_antes
            
            # Filtramos los que no sean temporales (.crdownload)
            validos = [f for f in nuevos if not f.endswith('.crdownload')]
            
            if validos:
                # ¡Encontramos el nuevo!
                posible_archivo = validos[0]
                # Verificamos que tenga tamaño > 0 (que haya terminado de escribirse)
                if os.path.getsize(posible_archivo) > 0:
                    downloaded_file = posible_archivo
                    break
            
            time.sleep(1)

        if not downloaded_file:
            return False, None, "Timeout: No se detectó un archivo nuevo."

        # Renombrar
        shutil.move(downloaded_file, target_path)
        
        print(f">>> [Sura] EXTRACCIÓN COMPLETADA.")
        print(f">>> [Sura] Archivo guardado en: {target_path}")

        return True, None, f"Archivo guardado: {target_filename}"

    except Exception as e:
        return False, None, f"Error Selenium Sura: {str(e)}"
    
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    s, _, m = run_extraction()
    print(f"\nEstado: {'EXITO' if s else 'FALLO'}")
    print(f"Mensaje: {m}")