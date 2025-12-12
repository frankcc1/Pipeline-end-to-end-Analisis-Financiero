import pandas as pd
import os
import logging
from datetime import datetime

# ==========================================
# CONFIGURACIÓN DE RUTAS Y LOGS
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, 'Data_Extraction', 'Raw_Files')
LOG_DIR = os.path.join(BASE_DIR, 'Data_Preparation', 'Logs')
GOLD_DIR = os.path.join(BASE_DIR, 'Data_God')

# Crear directorios si no existen
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

# Configuración del Logger
log_filename = os.path.join(LOG_DIR, f'transformation_log_{datetime.now().strftime("%Y%m%d")}.log')
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==========================================
# CLASE PRINCIPAL DE TRANSFORMACIÓN
# ==========================================
class TransformationManager:
    def __init__(self):
        self.raw_path = RAW_DIR
        self.gold_path = GOLD_DIR
        logging.info("Iniciando proceso de Transformación de Datos")

    def _save_to_gold(self, df, filename):
        """
        Guarda el dataframe en formato EXCEL (.xlsx) en la carpeta Data_God.
        """
        try:
            # Definir ruta de salida con extensión .xlsx
            output_path = os.path.join(self.gold_path, f"{filename}.xlsx")
            
            # Guardar en Excel (index=False para que no guarde la numeración de filas)
            df.to_excel(output_path, index=False)
            
            logging.info(f"Archivo guardado exitosamente: {filename}.xlsx - Registros: {len(df)}")
            print(f"✅ [SUCCESS] {filename}.xlsx generado correctamente en Data_God.")
            return True
            
        except Exception as e:
            logging.error(f"Error al guardar {filename}: {str(e)}")
            print(f"❌ [ERROR] Fallo al guardar {filename}: {e}")
            return False

    def process_treasury(self):
        """
        Procesa Treasury.xlsx
        """
        file_name = 'Treasury.xlsx'
        file_path = os.path.join(self.raw_path, file_name)
        
        logging.info(f"Procesando archivo: {file_name}")

        try:
            # 1. Validar existencia
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No se encuentra {file_path}")
            
            # 2. Lectura
            df = pd.read_excel(file_path)

            # 3. Transformación (Unpivot)
            df_transformed = df.melt(
                id_vars=['Date'], 
                var_name='Tenor', 
                value_name='Rate'
            )

            # 4. Limpieza y Formato
            df_transformed['Date'] = pd.to_datetime(df_transformed['Date'])
            df_transformed['Rate'] = pd.to_numeric(df_transformed['Rate'], errors='coerce')
            df_transformed['Curve_Type'] = 'Treasury_USD' 
            
            # Ordenar
            df_transformed.sort_values(by=['Date', 'Tenor'], inplace=True)

            # 5. Guardado
            self._save_to_gold(df_transformed, 'Treasury_Processed')
            return True # <--- OBLIGATORIO PARA EL PIPELINE

        except Exception as e:
            logging.error(f"Fallo en process_treasury: {str(e)}")
            print(f"❌ [ERROR] Fallo crítico en Treasury: {e}")
            return False # <--- OBLIGATORIO PARA EL PIPELINE

    def process_sbs_soberana(self):
        """
        Procesa sbs_soberana.xls
        """
        file_name = 'sbs_soberana.xls'
        file_path = os.path.join(self.raw_path, file_name)
        
        logging.info(f"Procesando archivo: {file_name}")

        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No se encuentra {file_path}")
            
            # 1. Lectura HTML
            try:
                dfs = pd.read_html(file_path, header=1, encoding='utf-8')
            except:
                dfs = pd.read_html(file_path, header=1, encoding='latin-1')
            
            if not dfs:
                raise ValueError("No se encontraron tablas")
            
            df = dfs[0]
            
            # 2. Limpieza de Fechas
            df['Fecha de Proceso'] = df['Fecha de Proceso'].astype(str).str.strip()
            df['Fecha de Proceso'] = pd.to_datetime(df['Fecha de Proceso'], dayfirst=True, errors='coerce')
            df.dropna(subset=['Fecha de Proceso'], inplace=True)
            
            # 3. Renombrar y Tipos
            df.rename(columns={
                'Fecha de Proceso': 'Date',
                'Indice de Spread': 'Rate',
                'Tipo de Curva': 'Tenor' 
            }, inplace=True)

            df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
            df['Curve_Type'] = 'SBS_Soberana_Soles' 

            # 4. Guardado
            cols_to_keep = ['Date', 'Tenor', 'Rate', 'Curve_Type', 'Clasificación']
            df_final = df[[c for c in cols_to_keep if c in df.columns]]
            
            self._save_to_gold(df_final, 'SBS_Soberana_Processed')
            return True # <--- OBLIGATORIO PARA EL PIPELINE

        except Exception as e:
            logging.error(f"Fallo en process_sbs_soberana: {str(e)}")
            print(f"❌ [ERROR] Fallo crítico en SBS Soberana: {e}")
            return False # <--- OBLIGATORIO PARA EL PIPELINE

    def process_sbs_bcrp(self):
        """
        Procesa sbs_bcrp.xls
        """
        file_name = 'sbs_bcrp.xls'
        file_path = os.path.join(self.raw_path, file_name)
        
        logging.info(f"Procesando archivo: {file_name}")

        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No se encuentra {file_path}")
            
            # 1. Lectura HTML
            try:
                dfs = pd.read_html(file_path, header=1, encoding='utf-8')
            except:
                dfs = pd.read_html(file_path, header=1, encoding='latin-1')
            
            if not dfs:
                raise ValueError("No se encontraron tablas válidas")
            
            df = dfs[0]

            # 2. Limpieza de Fechas
            df['Fecha de Proceso'] = df['Fecha de Proceso'].astype(str).str.strip()
            df['Fecha de Proceso'] = pd.to_datetime(df['Fecha de Proceso'], dayfirst=True, errors='coerce')
            df.dropna(subset=['Fecha de Proceso'], inplace=True)

            # 3. Renombrar Columnas
            df.rename(columns={
                'Fecha de Proceso': 'Date',
                'Plazo (DIAS)': 'Tenor',
                'Tasas (%)': 'Rate'
            }, inplace=True)

            # 4. Tipos
            df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
            df['Tenor'] = pd.to_numeric(df['Tenor'], errors='coerce')
            df['Curve_Type'] = 'CD_BCRP_Soles' 

            # 5. Guardado
            cols_to_keep = ['Date', 'Tenor', 'Rate', 'Curve_Type']
            df_final = df[[c for c in cols_to_keep if c in df.columns]]

            self._save_to_gold(df_final, 'SBS_BCRP_Processed')
            return True # <--- OBLIGATORIO PARA EL PIPELINE

        except Exception as e:
            logging.error(f"Fallo en process_sbs_bcrp: {str(e)}")
            print(f"❌ [ERROR] Fallo crítico en SBS BCRP: {e}")
            return False # <--- OBLIGATORIO PARA EL PIPELINE

    def process_curva_dolares(self):
        """
        Procesa curva_dolares.xls
        """
        file_name = 'curva_dolares.xls'
        file_path = os.path.join(self.raw_path, file_name)
        
        logging.info(f"Procesando archivo: {file_name}")

        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"No se encuentra {file_path}")
            
            # 1. Lectura HTML
            try:
                dfs = pd.read_html(file_path, header=1, encoding='utf-8')
            except:
                dfs = pd.read_html(file_path, header=1, encoding='latin-1')
            
            if not dfs:
                raise ValueError("No se encontraron tablas válidas")
            
            df = dfs[0]

            # 2. Limpieza de Fechas
            df['Fecha de Proceso'] = df['Fecha de Proceso'].astype(str).str.strip()
            df['Fecha de Proceso'] = pd.to_datetime(df['Fecha de Proceso'], dayfirst=True, errors='coerce')
            df.dropna(subset=['Fecha de Proceso'], inplace=True)

            # 3. Renombrar
            df.rename(columns={
                'Fecha de Proceso': 'Date',
                'Plazo (DIAS)': 'Tenor',
                'Tasas (%)': 'Rate'
            }, inplace=True)

            # 4. Tipos
            df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
            df['Tenor'] = pd.to_numeric(df['Tenor'], errors='coerce')
            df['Curve_Type'] = 'Curva_Dolares_CCSDF'

            # 5. Guardado
            cols_to_keep = ['Date', 'Tenor', 'Rate', 'Curve_Type']
            df_final = df[[c for c in cols_to_keep if c in df.columns]]
            
            self._save_to_gold(df_final, 'Curva_Dolares_Processed')
            return True # <--- OBLIGATORIO PARA EL PIPELINE

        except Exception as e:
            logging.error(f"Fallo en process_curva_dolares: {str(e)}")
            print(f"❌ [ERROR] Fallo crítico en Curva Dólares: {e}")
            return False # <--- OBLIGATORIO PARA EL PIPELINE

# ==========================================
# EJECUCIÓN (MODO PRUEBA UNITARIA)
# ==========================================
if __name__ == "__main__":
    transformer = TransformationManager()
    transformer.process_treasury()
    transformer.process_sbs_soberana()
    transformer.process_sbs_bcrp()
    transformer.process_curva_dolares()