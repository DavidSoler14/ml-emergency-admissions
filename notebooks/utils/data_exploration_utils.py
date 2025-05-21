import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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