import pandas as pd
from pathlib import Path

def read_clean_files() -> list:
    """"
    Lee los archivos parquet de la carpeta ../datasets/clean_datasets/ y devuelve una lista
    """
    folder = Path('../datasets/clean_datasets/')
    parquet_files = folder.glob('*.parquet') 
    
    return parquet_files

def cast_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se castea la columna de fecha a tipo datetime y se formatea a YYYY-MM-DD o YYYY-MM-DD HH:MM:SS

    Parameters:
    - df (pd.DataFrame): DataFrame con las fechas a castear

    Returns:
    - pd.DataFrame: DataFrame con las columnas de fecha casteadas
    """
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df['datetime'] = df['datetime'].dt.floor('min')  # Trunca a minutos

    return df

def save_processed_df(df: pd.DataFrame, name: str) -> None:
    """
        Guarda un DataFrame en un archivo parquet

        Parameters:
        - df (pd.DataFrame): formato del archivo.
        - name (str): nombre del archivo.
        Returns:
        - None
        """
    
    ruta_salida = f'../datasets/proccesed_datasets/{name}.parquet'
    
    try:
        # Guardar el DataFrame como un archivo Parquet
        df.to_parquet(
            ruta_salida,
            index=False,
        )

        print(f"Archivo guardado exitosamente en: {ruta_salida}")
        
    except Exception as e:
        print(f"Error al guardar el archivo '{name}': {e}")

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """"
    Procesa el DataFrame borrando eliminando duplicados, rellenando valores nulos y tratando los outliers

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    
def fill_missing_values(df) -> pd.DataFrame:
    """"
    Procesa el DataFrame borrando eliminando duplicados, rellenando valores nulos y tratando los outliers

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """