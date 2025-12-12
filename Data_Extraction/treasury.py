import pandas as pd
import requests
import io
import datetime
import os

def run_extraction():
    """
    FASE 1: EXTRACCIÓN PURA - TREASURY (Raw Data).
    1. Descarga el CSV oficial del Tesoro de EE.UU.
    2. Lo guarda físicamente como 'Treasury.xlsx' en 'Raw_Files'.
    NO devuelve DataFrame, solo confirma el guardado.
    """
    try:
        # --- 1. CONFIGURACIÓN DE RUTAS ---
        root_dir = os.getcwd()
        raw_dir = os.path.join(root_dir, "Raw_Files")
        
        if not os.path.exists(raw_dir): 
            os.makedirs(raw_dir)

        # Nombre del archivo objetivo
        target_filename = "Treasury.xlsx"
        target_path = os.path.join(raw_dir, target_filename)

        # --- 2. DESCARGA DEL CSV ---
        year = datetime.datetime.now().year
        print(f">>> [Treasury] Descargando data del año {year}...")
        
        # URL Oficial
        url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"
        
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            # Leemos el contenido CSV en memoria
            df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
            
            # --- 3. GUARDADO FÍSICO EN EXCEL ---
            # Si existe anterior, lo sobrescribe automáticamente
            # index=False para que no guarde el índice numérico de pandas
            df.to_excel(target_path, index=False)
            
            print(f">>> [Treasury] EXTRACCIÓN COMPLETADA.")
            print(f">>> [Treasury] Archivo guardado en: {target_path}")

            # Retornamos True, None (siguiendo el estándar de solo extracción)
            return True, None, f"Archivo guardado: {target_filename}"
            
        else:
            return False, None, f"Error HTTP {response.status_code} al descargar Treasury."

    except Exception as e:
        return False, None, f"Error crítico en Treasury: {str(e)}"

# Bloque de prueba
if __name__ == "__main__":
    s, _, m = run_extraction()
    print(f"\nEstado: {'EXITO' if s else 'FALLO'}")
    print(f"Mensaje: {m}")