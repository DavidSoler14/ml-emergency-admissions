import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from statsmodels.tsa.seasonal import seasonal_decompose
from scipy.stats import zscore, mode

def summarize_series(s, name):
    print(f"\nResumen de {name}:")
    print(f"Media: {s.mean():.4f}")
    print(f"Mediana: {s.median():.4f}")
    print(f"Desviación estándar: {s.std():.4f}")

def analyze_all_hospitals(df, period=7, z_thresh=3, plot=True):
    """
    Aplica descomposición estacional a cada hospital en el DataFrame.

    Parámetros:
        df (DataFrame): Debe tener columnas ['date' o 'datetime', 'hospital', 'admissions']
        period (int): Periodo estacional (por defecto 7)
        z_thresh (float): Umbral para detectar outliers con z-score
        plot (bool): Indica si se quiere crear gráfico por hospital

    Retorna:
        dict con resultados por hospital.
    """

    # Verificar columnas disponibles
    datetime_col = None
    if 'datetime' in df.columns:
        datetime_col = 'datetime'
    elif 'date' in df.columns:
        datetime_col = 'date'
    else:
        raise ValueError("El DataFrame debe tener una columna 'date' o 'datetime'")

    if not {'hospital', 'admissions'}.issubset(df.columns):
        raise ValueError("El DataFrame debe tener columnas: 'hospital', 'admissions', y 'date' o 'datetime'")

    df = df.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df = df.sort_values(by=datetime_col)

    results_by_hospital = {}

    for hospital in df['hospital'].unique():
        df_hosp = df[df['hospital'] == hospital]
        series = df_hosp.set_index(datetime_col)['admissions'].sort_index()

        original_na = series[series.isna()].index
        series_interp = series.interpolate(method='linear')

        if len(series_interp.dropna()) < period * 2:
            print(f"El hospital '{hospital}' tiene pocos datos para descomponer.")
            continue

        try:
            result = seasonal_decompose(series_interp, model='additive', period=period)
        except Exception as e:
            print(f"Error al descomponer '{hospital}': {e}")
            continue

        trend = result.trend
        seasonal = result.seasonal
        resid = result.resid

        trend = result.trend
        seasonal = result.seasonal
        resid = result.resid

        # Mostrar resumen estadístico de componentes
        summarize_series(trend.dropna(), "Tendencia")
        summarize_series(seasonal.dropna(), "Estacionalidad")
        summarize_series(resid.dropna(), "Ruido (Residuo)")

        # Outliers
        resid_clean = resid.dropna()
        resid_z = zscore(resid_clean)
        outliers_index = resid_clean[np.abs(resid_z) > z_thresh].index

        # NaNs y puntos problemáticos
        nan_in_decomp = trend[trend.isna()].index.union(
            seasonal[seasonal.isna()].index).union(
            resid[resid.isna()].index).union(original_na)

        imputar_index = nan_in_decomp.union(outliers_index)

        if plot:
            start_date = series.index.min()
            end_date = series.index.max()
            diff_years = (end_date - start_date).days / 365.25

            if diff_years > 2:
                fig = plt.figure(figsize=(18, 8))
                gs = fig.add_gridspec(1, 2, width_ratios=[1, 1])

                ax1 = fig.add_subplot(gs[0, 0])
                ax2 = fig.add_subplot(gs[0, 1])

                locator_full = mdates.MonthLocator(interval=6)
                formatter_full = mdates.DateFormatter('%Y-%m')
                plot_decomposition(result, ax1, locator_full, formatter_full)


                series.index = series.index.normalize() 
                end_date = series.index.max()
                last_year_start = end_date - pd.Timedelta(days=365)
                series_last_year = series[series.index >= last_year_start]


                try:
                    result_last_year = seasonal_decompose(series_last_year.interpolate(method='linear'),
                                                          model='additive', period=period)
                    locator_year = mdates.MonthLocator()
                    formatter_year = mdates.DateFormatter('%Y-%m')
                    plot_decomposition(result_last_year, ax2, locator_year, formatter_year)
                except Exception as e:
                    print(f"Error al descomponer último año de '{hospital}': {e}")
                    ax2.remove()

                fig.suptitle(f"Descomposición - {hospital}", fontsize=14)
                plt.tight_layout(rect=[0, 0.03, 1, 0.95])
                plt.show()

            else:
                # Para <= 2 años
                plt.figure(figsize=(16, 6))
                result.plot()
                plt.suptitle(f"Descomposición - {hospital}", fontsize=14)

                for ax in plt.gcf().axes:
                    ax.tick_params(axis='x', rotation=45, labelsize=9)
                    ax.xaxis.set_major_locator(mdates.MonthLocator())
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

                plt.tight_layout()
                plt.show()

        results_by_hospital[hospital] = {
            'trend': trend,
            'seasonal': seasonal,
            'resid': resid,
            'outliers_index': outliers_index,
            'missing_index': original_na,
            'imputar_index': imputar_index
        }


    return results_by_hospital

def plot_decomposition(result, ax, locator, formatter):
    """
    Grafica las 4 componentes de la descomposición en sub-ejes verticales dentro de ax.
    """
    gs = gridspec.GridSpecFromSubplotSpec(4, 1, subplot_spec=ax.get_subplotspec(), hspace=0.1)
    components = ['observed', 'trend', 'seasonal', 'resid']

    for i, comp in enumerate(components):
        sub_ax = plt.Subplot(ax.figure, gs[i])
        ax.figure.add_subplot(sub_ax)

        series = getattr(result, comp)
        sub_ax.plot(series.index, series.values, label=comp)
        sub_ax.set_ylabel(comp)

        if i < 3:
            sub_ax.set_xticklabels([])
        else:
            sub_ax.tick_params(axis='x', rotation=45)
            sub_ax.xaxis.set_major_locator(locator)
            sub_ax.xaxis.set_major_formatter(formatter)

    ax.remove() 


def show_df_metrics(df: pd.DataFrame) -> None:
    """
    Muestra las métricas del DataFrame

    Parameters:
    - df (pd.DataFrame): DataFrame a estudiar
    """
    df.info()
    df.describe(include='all')
    df.head()

    print(" \nPorcentaje de nulos por columna:")
    print((df.isnull().mean() * 100).sort_values(ascending=False))

    print(" \nDuplicados:")
    print(df.duplicated().sum())

def print_graph(df: pd.DataFrame) -> None:
    """
    Muestra gráficos de los datos

    Parameters:
    - df (pd.DataFrame): DataFrame a estudiar
    """
    # Se crea el gráfico
    plt.figure(figsize=(14, 6))
    df.plot(kind='line', ax=plt.gca())
    plt.title("Admisiones en urgencias por hospital a lo largo del tiempo")
    plt.xlabel("Fecha")
    plt.ylabel("Número de admisiones")
    plt.grid(True)
    plt.legend(title='Hospital')
    plt.tight_layout()
    plt.show()


def print_top10_graph(df: pd.DataFrame) -> None:

    top_hospitals = df.groupby('hospital')['admissions'].sum().sort_values(ascending=False).head(10).index

    # Se cre columna nueva para agrupar "Otros"
    df_top = df[df['hospital'].isin(top_hospitals)]

    # Se crea el gráfico
    df_pivot_top = df_top.pivot(index='date', columns='hospital', values='admissions')
    plt.figure(figsize=(14, 6))
    df_pivot_top.plot(ax=plt.gca())
    plt.title("Top 10 hospitales con más admisiones a lo largo del tiempo")
    plt.xlabel("Fecha")
    plt.ylabel("Número de admisiones")
    plt.grid(True)
    plt.legend(title='Hospital')
    plt.tight_layout()
    plt.show()