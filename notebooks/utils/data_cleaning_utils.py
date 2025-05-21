import pandas as pd
import glob
import os

def read_raw_data(format: str, file_path: str, options: dict, large_file: bool) -> pd.DataFrame:
    """
    Lee datos dado una ruta de archivo y un formato

    Parameters:
    - format (str): formato del archivo.
    - file_path (str): ruta del archivo.
    - options (dict): Diccionario con las opciones de lectura
    - large_file (bool): Indica si el archivo es grande 

    Returns:
    - pd.DataFrame: Un DataFrame con los datos leídos
    """

    print(f"Leyendo archivo: {file_path}")
    if large_file:
        df= read_large_file(format, file_path, options)

    else:
        if format == 'csv':
            if options:
                df = pd.read_csv(file_path, **options)
            else:
                df = pd.read_csv(file_path)
        elif format == 'txt':
            if options:
                df = pd.read_csv(file_path, **options)
            else:
                df = pd.read_csv(file_path)
        elif format in ['xls', 'xlsx']:
            if options:
                df = pd.read_excel(file_path, **options)
            else:
                df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Formato no soportado: {format}")
    
    return df

def read_large_file(format: str, file_path: str, options: dict) -> pd.DataFrame:
    """
    Lee archivos grandes en chunks y los concatena

    Parameters:
    - format (str): formato del archivo.
    - file_path (str): ruta del archivo.
    - options (dict): Diccionario con las opciones de lectura

    Returns:
    - pd.DataFrame: Un DataFrame con los datos leídos
    """
    chunksize = 10_000 

    if format == 'csv':        
        chunks = pd.read_csv(file_path, chunksize=chunksize, **(options or {}), on_bad_lines='skip')
        return pd.concat(chunk for chunk in chunks)
    elif format == 'txt':
        chunks = pd.read_csv(file_path, chunksize=chunksize, **(options or {}), on_bad_lines='skip')
        return pd.concat(chunk for chunk in chunks)
    else:
        raise ValueError("Formato no soportado")


def read_multi_file_paths(format: str, name: str) -> list:
    ruta_entrada = f'../datasets/raw_datasets/*{name}*.{format}'

    matching_files = glob.glob(ruta_entrada)

    if not matching_files:
        raise FileNotFoundError(f"No se encontraron archivos para el patrón: {ruta_entrada}")
    
    return matching_files

def save_clean_data(df: pd.DataFrame, name: str) -> None:
    """
        Guarda un DataFrame en un archivo parquet

        Parameters:
        - df (pd.DataFrame): formato del archivo.
        - name (str): nombre del archivo.
        Returns:
        - None
        """
    
    ruta_salida = f'../datasets/clean_datasets/{name}.parquet'
    
    try:
        # Guardar el DataFrame como un archivo Parquet
        df.to_parquet(
            ruta_salida,
            index=False,
        )

        print(f"Archivo guardado exitosamente en: {ruta_salida}")
        
    except Exception as e:
        print(f"Error al guardar el archivo '{name}': {e}")

def process_australia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Australia

    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    columns = df.columns.to_list()
    new_columns = []
    last_hospital = None

    for col in columns:
        # Si las columnas no son tuplas aún, crea las tuplas desde filas 0 y 1
        if not isinstance(col, tuple):
            raise ValueError("Las columnas deben tener dos niveles (usa header=[0,1] al leer el CSV)")
            
        hospital, campo = col  # hospital name (o 'Unnamed'), campo (como 'Tri_1')

        # Si hospital no es Unnamed, lo recordamos
        if not str(hospital).startswith("Unnamed"):
            last_hospital = hospital

        # Usamos el último hospital válido
        new_columns.append(f"{last_hospital}_{campo}" if campo != "Date" else "Date")

    # Asignar los nuevos nombres de columna
    df.columns = new_columns

    hospitales = set()
    for col in df.columns:
        if col != "Date":
            partes = col.split("_")
            hospital = "_".join(partes[:-1])  
            hospitales.add(hospital)

    # Filtro para quedarte solo con los nombres correctos
    hospitales = sorted([
                h for h in hospitales 
                if not h.endswith("_Tri")
            ])
    
    # Se crea un diccionario de DataFrames por hospital
    dfs_por_hospital = {}

    for hospital in hospitales:
        cols_hospital = [col for col in df.columns if col.startswith(hospital + "_")]
        df_hospital = df[["Date"] + cols_hospital].copy()
        
        new_names = {
                col: col.replace(hospital + "_", "") for col in cols_hospital
            }
        df_hospital = df_hospital.rename(columns=new_names)
                
        # Se agrega la columna de hospital
        df_hospital["Hospital"] = hospital
                
        dfs_por_hospital[hospital] = df_hospital

    # Se concatenan todos los DataFrames verticalmente
    df_final = pd.concat(dfs_por_hospital.values(), ignore_index=True)
    columnas_deseadas = ['Date', 'Admissions', 'Hospital']
    df_filtrado = df_final[columnas_deseadas]

    df_filtrado = df_filtrado.rename(columns={
    'Date': 'date',
    'Admissions': 'admissions',
    'Hospital': 'hospital'
    })

    print(f"DataFrame procesado con {len(df_filtrado)} filas y {len(df_filtrado.columns)} columnas.")
    return df_filtrado

def process_cardiff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Cardiff
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df = df.rename(columns={
    'arrival_1h': 'datetime',
    'n_attendance': 'admissions'
    })
    df['hospital'] = 'Cardiff Hospital'
    print(f"DataFrame procesado con {len(df)} filas y {len(df.columns)} columnas.")
    
    return df

def process_chile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Chile
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """

    # Se convierte la columna fecha a tipo datetime
    df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

    # Se crea una nueva columna que concatena ID y nombre
    df['Establecimiento'] = df['IdEstablecimiento'].astype(str) + " - " + df['NEstablecimiento'].astype(str)

    # Agrupar por Establecimiento y fecha, y sumar el total
    df_agrupado = df.groupby(['Establecimiento', 'fecha'])['Total'].sum().reset_index()

    # Se ordena el resultado
    df_agrupado = df_agrupado.sort_values(by=['Establecimiento', 'fecha'])

    df_agrupado = df_agrupado.rename(columns={
    'fecha': 'date',
    'Total': 'admissions',
    'Establecimiento': 'hospital'
    })
    df_ordenado = df_agrupado[['date', 'admissions', 'hospital']]

    print(f"DataFrame procesado con {len(df_ordenado)} filas y {len(df_ordenado.columns)} columnas.")
    return df_ordenado

def process_colombia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Colombia
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    # Se convierte la columna a datetime y se extrae la hora
    df['Hora_Ingre'] = pd.to_datetime(df['Hora_Ingre'], dayfirst=False,  errors='coerce')

    df['hora_str'] = df['Hora_Ingre'].dt.strftime('%H:%M:%S')

    df['Fecha_Ing'] = pd.to_datetime(df['Fecha_Ing'], dayfirst=False, errors='coerce')

    # Combina fecha y hora
    df['datetime'] = pd.to_datetime(
        df['Fecha_Ing'].dt.strftime('%Y-%m-%d') + ' ' + df['hora_str'],
        errors='coerce'
    )

    # Agrupar por ips y datetime, y sumar el total
    df_agrupado = df.groupby(['Ips', 'datetime']).size().reset_index(name='admissions')

    # Se ordena el resultado
    df_agrupado = df_agrupado.sort_values(by=['Ips', 'datetime'])

    df_agrupado = df_agrupado.rename(columns={
    'Ips': 'hospital'
    })
    df_ordenado = df_agrupado[['datetime', 'admissions', 'hospital']]

    return df_ordenado

def process_col_betania(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Betania
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_agrupado = df.groupby('FechaAtencion').size().reset_index(name='admissions')
    df_agrupado['hospital'] = 'Hospital de Betania'
    df_agrupado = df_agrupado.rename(columns={
        'FechaAtencion': 'date'
    })
    df_ordenado = df_agrupado[['date', 'admissions', 'hospital']]
    
    return df_ordenado

def process_esp_canarias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Canarias
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_agrupado = df.groupby(['fecha', 'codigo'])['valor'].sum().reset_index()
  
    diccionario_hospitales = {
    '380201': 'Hospital Quironsalud Vida',
    '380103': 'Hospital Parque',
    '380359': 'Clínica Tara',
    '380282': 'Hospital Quirón Salud Costa Adeje',
    '380146': 'Hospital Universitario Hospiten Sur',
    '380223': 'Hospital Universitario Hospiten Bellevue',
    '380099': 'Vithas Hospital Santa Cruz',
    '380295': 'Hospital Universitario Hospiten Rambla',
    '380086': 'Hospital Quirón Salud Tenerife',
    '380064': 'Hospital San Juan de Dios Tenerife',
    '380178': 'Hospital Universitario de Canarias',
    '380027': 'Hospital Universitario Nuestra Señora de Candelaria',
    '380276': 'Hospital Insular Nuestra Señora de Los Reyes',
    '380244': 'Hospital Nuestra Señora de Guadalupe',
    '380316': 'Hospital General de La Palma',
    '380348': 'Hospital del Sur',
    '380337': 'Hospital del Norte',
    '380033': 'Hospital de Ofra',
    '380048': 'Unidades Clínicas y de Rehabilitación de Salud Mental',
    '350167': 'Clínica Cajal',
    '350287': 'Hospital Ciudad de Telde',
    '350154': 'Hospital La Paloma',
    '350206': 'Hospital Perpetuo Socorro',
    '350304': 'Hospiten Clínica Roca San Agustín',
    '350115': 'Hospital San José',
    '350120': 'Vithas Hospital Santa Catalina',
    '350326': 'Hospiten Lanzarote',
    '350265': 'Quinta Medica de Reposo',
    '350367': 'Hospital Universitario San Roque Maspalomas',
    '350173': 'Hospital Universitario San Roque Las Palmas',
    '350389': 'Hospital Parque Fuerteventura',
    '350192': 'Clínica Bandama',
    '350311': 'Hospital Universitario de Gran Canaria Dr. Negrín',
    '350290': 'Complejo Hospitalario Universitario Insular Materno Infantil',
    '350252': 'Hospital General de Fuerteventura',
    '350390': 'Clínica Jorgani',
    '350228': 'Hospital Universitario Dr. José Molina Orosa',
    '350271': 'Hospital San Roque de Guía',
    '350069': 'Hospital Universitario de Salud Mental Juan Carlos I',
    '350249': 'Hospital Insular de Lanzarote',
    '350401': 'Centro Sanitario Residencial Las Palmas (Cesar Las Palmas)',
    '350402': 'Hospital Polivalente Anexo Juan Carlos I'
    }

    df_agrupado['codigo'] = df_agrupado['codigo'].astype(str).str.strip()
    df_agrupado['hospital'] = df_agrupado['codigo'].map(diccionario_hospitales).fillna(df_agrupado['codigo'])
    
    df_agrupado = df_agrupado.rename(columns={
        'fecha': 'date',
        'valor': 'admissions'
    })
    df_ordenado = df_agrupado[['date', 'admissions', 'hospital']].copy()


    df_ordenado['date'] = pd.to_datetime(df_ordenado['date'], dayfirst=True, errors='coerce')
    df_ordenado['date'] = df_ordenado['date'].dt.strftime('%Y-%m-%d')
                        
    return df_ordenado

def process_esp_castilla_y_leon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Castilla y León
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_agrupado = df.groupby(['Fecha de atención', 'Hospital']).size().reset_index(name='admissions')

    df_agrupado = df_agrupado.rename(columns={
        'Fecha de atención': 'date',
        'Hospital': 'hospital'
    })
    df_ordenado = df_agrupado[['date', 'admissions', 'hospital']].copy() 
  
    #Se lee el archivo del path destino y se hace el merge, debido a que existen dos fuentes de datos de españa
    target_path = f'../datasets/clean_datasets/spain_data.parquet'
    df_target = pd.read_parquet(target_path)
    
    df_final = pd.concat([df_target, df_ordenado], ignore_index=True).drop_duplicates()

    return df_final

def process_iowa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Iowa
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.rename(columns={df.columns[0]: 'year-hour'}, inplace=True)

    columnas_deseadas = ['year-hour', 'Jan.', 'Feb.', 'Mar.', 'Apr.', 'May.', 'Jun.', 'Jul.', 'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.']

    # Se filtran las columanas las columnas que contienen alguno de esos meses
    columnas_meses = [col for col in df.columns if any(mes in str(col) for mes in columnas_deseadas)]

    # Selecciona solo esas columnas
    df_filtrado = df[columnas_meses]

    df_filtrado = df_filtrado.copy()

    # Se extrae el año
    df_filtrado['year'] = df_filtrado['year-hour'].astype(str).where(df_filtrado['year-hour'].astype(str).str.fullmatch(r'\d{4}'))

    # Rellena hacia adelante
    df_filtrado['year'] = df_filtrado['year'].ffill()

    # Se extraen las horas
    hora_pattern = r'^\d{1,2} (AM|PM)$'
    df_filtrado['hour'] = df_filtrado['year-hour'].astype(str).where(df_filtrado['year-hour'].astype(str).str.match(hora_pattern))


    df_filtrado = df_filtrado.drop(columns=['year-hour'])

    df_filtrado = df_filtrado.dropna(subset=['hour'])
    df_filtrado = df_filtrado.melt(id_vars=['year', 'hour'], var_name='month_day', value_name='value')

    df_filtrado['month_day_clean'] = df_filtrado['month_day'].str.replace('.', ' ', regex=False)

    # Combina year, month_day_clean y hour en un string
    datetime_str = df_filtrado['year'].astype(str) + ' ' + df_filtrado['month_day_clean'] + ' ' + df_filtrado['hour']

    # Convierte el string a datetime
    df_filtrado['datetime'] = pd.to_datetime(datetime_str, format='%Y %b %d %I %p', errors='coerce')


    df_final = df_filtrado[['datetime', 'value']].copy()
    df_final['hospital'] = 'Iowa Hospital'
    df_final = df_final.rename(columns={
            'value': 'admissions'
        })
    df_ordenado = df_final[['datetime', 'admissions', 'hospital']]
    
    return df_ordenado

def process_iran(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Iran
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df['datetime_str'] = df['ResidentDate_year'].astype(str) + '-' + \
                     df['ResidentDate_month'].astype(str) + '-' + \
                     df['ResidentDate_day'].astype(str) + ' ' + \
                     df['ResidentDate_hour'].astype(str) + ':00:00'

    df['datetime'] = pd.to_datetime(df['datetime_str'])

    df_agrupado = df.groupby(['datetime']).size().reset_index(name='admissions')

    df_agrupado['hospital'] = 'Isfahan University Hospital'

    df_ordenado = df_agrupado[['datetime', 'admissions', 'hospital']]

    return df_ordenado


def process_mexico_2009(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2009
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 15, 18, 19]].copy()
    df_filtrado.columns = ["hospital", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df_filtrado= mexico_convert_date_hour_minute(df_filtrado)
    df_agrupado = df_filtrado.groupby(['hospital', 'datetime']).size().reset_index(name='admissions')
    df_ordenado = df_agrupado[['datetime', 'admissions', 'hospital']]

    return df_ordenado

def process_mexico_2010(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2010
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 18, 21, 22]].copy()
    df_filtrado.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df_filtrado= mexico_convert_date_hour_minute(df_filtrado)
    df_final= generic_mexico_append(df_filtrado)

    return df_final

def process_mexico_2011(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2011
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 18, 21, 22]].copy()
    df_filtrado.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df_filtrado= mexico_convert_date_hour_minute(df_filtrado)
    df_final= generic_mexico_append(df_filtrado)

    return df_final

def process_mexico_2012(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2012
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 19, 22, 23]].copy()
    df_filtrado.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df_filtrado= mexico_convert_date_hour_minute(df_filtrado)
    df_final= generic_mexico_append(df_filtrado)

    return df_final

def process_mexico_2013(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2013
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 19, 22, 23]].copy()
    df_filtrado.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]
    
    df_filtrado= mexico_convert_date_hour_minute(df_filtrado)
    df_final= generic_mexico_append(df_filtrado)

    # df_agrupado = df_filtrado.groupby(['hospital', 'date']).size().reset_index(name='admissions')
    # df_ordenado = df_agrupado[['date', 'admissions', 'hospital']]
  
    # #Se lee el archivo del path destino y se hace el merge, debido a que existen dos fuentes de datos de españa
    # target_path = f'../datasets/clean_datasets/mexico_data.parquet'
    # df_target = pd.read_parquet(target_path)
    
    # df_final = pd.concat([df_target, df_ordenado], ignore_index=True).drop_duplicates()

    return df_final

def process_mexico_2014(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2014
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df.iloc[:, [1, 19, 22, 23]].copy()
    df_filtrado.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df= mexico_convert_date_hour_minute(df_filtrado)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2015(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2015
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df= mexico_convert_date_hour_minute(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2016(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2016
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df= mexico_convert_date_hour_minute(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2017(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2017
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df= mexico_convert_date_hour_minute(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2018(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2018
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]

    df= mexico_convert_date_hour_minute(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2019(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2019
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df.columns = ["CLUES", "FECHAINGRESO", "HORA_INGRESO", "MINUTO_INGRESO"]
    
    df= mexico_convert_date_hour_minute(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2020(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2020
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df = df.rename(columns={"fechaingreso": "FECHAINGRESO", "hora_ingreso": "HORA_INGRESO"})

    df= mexico_convert_date_hour(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2021(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2021
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df= mexico_convert_date_hour(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2022(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2022
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df = df.rename(columns={"fechaingreso": "FECHAINGRESO", "hora_ingreso": "HORA_INGRESO"})

    df= mexico_convert_date_hour(df)
    df_final= generic_mexico_append(df)

    return df_final

def process_mexico_2023(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de México de 2023
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df = df.rename(columns={"fechaingreso": "FECHAINGRESO", "hora_ingreso": "HORA_INGRESO"})

    df= mexico_convert_date_hour(df)
    df_final= generic_mexico_append(df)

    return df_final

def generic_mexico_append(df: pd.DataFrame) -> pd.DataFrame:

    df_filtrado = df[["FECHAINGRESO", "CLUES"]]
    df_filtrado.columns = ["datetime", "hospital"]

    df_agrupado = df_filtrado.groupby(['hospital', 'datetime']).size().reset_index(name='admissions')
    df_ordenado = df_agrupado[['datetime', 'admissions', 'hospital']]
  
    #Se lee el archivo del path destino y se hace el merge, debido a que existen dos fuentes de datos de mexico
    target_path = f'../datasets/clean_datasets/mexico_data.parquet'
    df_target = pd.read_parquet(target_path)
    
    df_final = pd.concat([df_target, df_ordenado], ignore_index=True).drop_duplicates()

    return df_final

def mexico_convert_date_hour_minute(df):
    df= df.copy()
    df["FECHAINGRESO"] = pd.to_datetime(df["FECHAINGRESO"], errors="coerce")
    df["HORA_INGRESO"] = pd.to_numeric(df["HORA_INGRESO"], errors="coerce").clip(upper=23).fillna(0).astype(int)
    df["MINUTO_INGRESO"] = pd.to_numeric(df["MINUTO_INGRESO"], errors="coerce").clip(upper=59).fillna(0).astype(int)

    df["datetime"] = df["FECHAINGRESO"] + pd.to_timedelta(df["HORA_INGRESO"], unit='h') + pd.to_timedelta(df["MINUTO_INGRESO"], unit='m')

    return df

def mexico_convert_date_hour(df):
    df= df.copy()
    df["FECHAINGRESO"] = pd.to_datetime(df["FECHAINGRESO"], errors="coerce")
    # Crea columna 'datetime' combinando la fecha y la hora
    df["datetime"] = pd.to_datetime(
    df["FECHAINGRESO"].dt.strftime("%Y-%m-%d") + " " + df["HORA_INGRESO"],
    errors="coerce"
    )
    return df

def process_pak_(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Pakistan
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df[df['country'].str.contains('pak', na=False)].copy()
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_agrupado = df.groupby(['date', 'hospital'])['attendences'].sum().reset_index()
    df_final = df_agrupado.rename(columns={
                'attendences': 'admissions'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    return df_final

def process_usa_(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de USA
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df[df['country'].str.contains('usa', na=False)].copy()
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_agrupado = df.groupby(['date', 'hospital'])['attendences'].sum().reset_index()
    df_final = df_agrupado.rename(columns={
                'attendences': 'admissions'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    return df_ordenado

def process_nl_(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Netherlands
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df[df['country'].str.contains('nl', na=False)].copy()
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_agrupado = df.groupby(['date', 'hospital'])['attendences'].sum().reset_index()
    df_final = df_agrupado.rename(columns={
                'attendences': 'admissions'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    return df_final

def process_bwa_(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Botswana
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df[df['country'].str.contains('bot', na=False)].copy()
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_agrupado = df.groupby(['date', 'hospital'])['attendences'].sum().reset_index()
    df_final = df_agrupado.rename(columns={
                'attendences': 'admissions'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    return df_final

def process_aus_(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Australia
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    df_filtrado = df[df['country'].str.contains('aus', na=False)].copy()
    df_filtrado['date'] = pd.to_datetime(df_filtrado['date'], format='%Y%m%d')
    df_agrupado = df.groupby(['date', 'hospital'])['attendences'].sum().reset_index()
    df_final = df_agrupado.rename(columns={
                'attendences': 'admissions'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    #Se lee el archivo del path destino y se hace el merge, debido a que existen dos fuentes de datos de usa
    target_path = f'../datasets/clean_datasets/australia_data.parquet'
    df_target = pd.read_parquet(target_path)
    
    df_final = pd.concat([df_target, df_ordenado], ignore_index=True).drop_duplicates()

    return df_final

def process_wales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos de Gales
    Parameters:
    - df (pd.DataFrame): DataFrame con los datos a procesar

    Returns:
    - pd.DataFrame: DataFrame procesado
    """
    # Se normalizan los nombres de las columnas
    df.columns = df.columns.str.strip()

    # Se filtran las filas que contienen datos numéricos en la columna "Wales" para evitar seleccionar los metadatos
    df_filtrado = df[pd.to_numeric(df["Wales"], errors="coerce").notna()]

    df_filtrado = df_filtrado.copy()

    df_filtrado = df_filtrado.rename(columns={df_filtrado.columns[0]: "Date"})


    df_filtrado["Date"] = df_filtrado["Date"].astype(str).str.strip().str.replace('"', '')
    df_filtrado = df_filtrado[df_filtrado["Date"].str.match(r"^\d")]

    df_final = df_filtrado.melt(
        id_vars=["Date"],                 
        value_vars=[col for col in df_filtrado.columns if col != "Date"],
        var_name="Hospital",
        value_name="Admissions"
    )

    df_final = df_final[df_final["Hospital"] != "Wales"]
    df_final = df_final.rename(columns={
                'Date': 'date',
                'Admissions': 'admissions',
                'Hospital': 'hospital'
            })
    df_ordenado = df_final[['date', 'admissions', 'hospital']]

    return df_ordenado



