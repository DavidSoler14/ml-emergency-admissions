import pandas as pd

def cast_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se castea la columna de fecha a tipo datetime y se formatea a YYYY-MM-DD
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        
    return df