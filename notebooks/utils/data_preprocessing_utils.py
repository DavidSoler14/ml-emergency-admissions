import pandas as pd
from pathlib import Path

def read_clean_files() -> list:
    """"
    Lee los archivos parquet de la carpeta ../datasets/clean_datasets/ y devuelve una lista
    """
    folder = Path('../datasets/clean_datasets/')
    parquet_files = folder.glob('*.parquet') 

    return parquet_files

def cast_columns_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se castea los tipos de datos de las columnas

    Parameters:
    - df (pd.DataFrame): DataFrame con las fechas a castear

    Returns:
    - pd.DataFrame: DataFrame con las columnas de fecha casteadas
    """
    if 'date' in df.columns:
    # Limpiar y convertir la columna a string
        df['date'] = df['date'].astype(str).str.strip()
        date_sample = df['date'].dropna()

        # Detectar si la mayoría de las fechas parecen estar en formato YYYYMMDD
        if not date_sample.empty and (date_sample.str.match(r'^\d{8}$').mean() > 0.8):
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
        else:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df['datetime'] = df['datetime'].dt.floor('min')  # Trunca a minutos

    # Se convierte la columna 'admissions' a numérico
    df['admissions'] = pd.to_numeric(df['admissions'], errors='coerce')

    # Se convierte la columna 'hospital' a string
    df['hospital'] = df['hospital'].astype(str)

    print(f"Columnas casteadas")

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
    
    ruta_salida = f'../datasets/processed_datasets/{name}.parquet'
    
    try:
        # Guardar el DataFrame como un archivo Parquet
        df.to_parquet(
            ruta_salida,
            index=False,
        )

        print(f"Archivo guardado exitosamente en: {ruta_salida}")
        
    except Exception as e:
        print(f"Error al guardar el archivo '{name}': {e}")

def group_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa los datos por hospital y por fecha (puede ser 'date' o 'datetime'), sumando las admisiones.

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    # Determina si se agrupa por 'datetime' o por 'date'
    time_col = 'datetime' if 'datetime' in df.columns else 'date'

    # Agrupa por hospital y la columna temporal elegida
    df_grouped = df.groupby(['hospital', time_col], as_index=False).agg({'admissions': 'sum'})

    # Ordena por hospital y fecha/hora
    df_ordered = df_grouped.sort_values(by=['hospital', time_col], ascending=[True, True])

    print(f"Datos agrupados y ordenados por hospital y fecha. Total de filas: {len(df_ordered)}")
    return df_ordered

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """"
    Procesa el DataFrame borrando eliminando duplicados, rellenando valores nulos y tratando los outliers

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df= fill_missing_values(df)
    print("Se ha tratado los valores nulos")

    df= remove_outliers(df, "admissions").reset_index(drop=True)
    print("Se han eliminado los outliers")

    return df

def fill_missing_values(df) -> pd.DataFrame:
    """"
    Procesa el DataFrame rellenando valores nulos y tratando los outliers

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df['admissions'] = df.groupby('hospital')['admissions'].transform(lambda x: x.ffill())

    return df

def remove_outliers(df: pd.DataFrame, column: str, factor: float = 1.5) -> pd.DataFrame:
    """
    Elimina outliers de una columna numérica usando el método del rango intercuartílico (IQR).

    Parameters:
    - df (pd.DataFrame): DataFrame de entrada
    - column (str): Nombre de la columna sobre la que se aplicará la detección de outliers

    Returns:
    - pd.DataFrame: DataFrame sin los outliers detectados
    """
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    
    avg = df[column].mean()
    std = df[column].std()
    low = avg - 2 * std
    high = avg + 2 * std
    df = df[df[column].between(low, high, inclusive='both')]

    return df

def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa el DataFrame agregando características temporales útiles para modelado ARIMAX:
    - Valor del mismo día de la semana anterior (lag 7)
    - Media de admisiones de la semana anterior (rolling window 7 días, excluyendo el día actual)
    - Día de la semana (0-6)
    - Indicador si la fecha es fin de semana
    - Estación del año
    Luego agrupa por hospital y fecha, sumando admisiones y ordena.

    Parámetros:
    - df: pd.DataFrame con columnas ['hospital', 'date', 'admissions']

    Retorna:
    - pd.DataFrame procesado con nuevas columnas
    """
    time_col = 'datetime' if 'datetime' in df.columns else 'date'
    df['day_of_week'] = df[time_col].dt.dayofweek

    # Indicador si es fin de semana (sábado=5, domingo=6)
    df['is_weekend'] = df['day_of_week'].isin([5,6]).astype(int)

    # Función para determinar estación del año en hemisferio norte
    def get_season(date):
        md = date.month * 100 + date.day
        if (md >= 321) and (md <= 620):
            return 1
        elif (md >= 621) and (md <= 922):
            return 2
        elif (md >= 923) and (md <= 1220):
            return 3
        else:
            return 4
    df['season'] = df[time_col].apply(get_season)

    # Para cada hospital calculamos lag 7 (misma fecha semana anterior) y rolling 7 excluyendo el día actual
    def add_lags(group):
        group = group.sort_values(time_col)
        group['lag_7'] = group['admissions'].shift(7)
        group['lag_14'] = group['admissions'].shift(14)

        # Rolling 7 días anteriores, excluyendo día actual (window=7, closed='left')
        group['rolling_7'] = group['admissions'].shift(1).rolling(window=7).mean()
        group['rolling_14'] = group['admissions'].shift(1).rolling(window=14).mean()
        return group

    df = df.groupby('hospital').apply(add_lags).reset_index(drop=True)

    df = df.sort_values(['hospital', time_col]).reset_index(drop=True)

    print(f"Se han llevado a cabo las agregaciones")

    return df


    