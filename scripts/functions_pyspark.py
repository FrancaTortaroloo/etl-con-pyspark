#%%importamos librerias que necesitamos para la lectura, conversión y limpieza de datos

import pyspark
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

#%%
#leer el archivo 
def leer_archivo(ruta_archivo):
    spark = SparkSession.builder.appName("chanchito-feliz").getOrCreate()
    app = spark.read.csv(path        = ruta_archivo,
                         inferSchema = True, header = True)
    app.show(20, truncate = True)
    return app


#%%

def exploracion_datos(csv):
    print('_____________ INFORMACIÓN GENERAL DEL CSV ____________\n')

    print(f"El número de filas que tenemos es de {csv.count()}.\nEl número de columnas es de {len(csv.columns)}\n")

   
    print('____________ DUPLICADOS EN CADA COLUMNA ______________ \n')
    
    #contar numero total de filas 

    total_filas = csv.count()

    #lsta para almacenar las columnas que tienen duplicaods 

    columnas_con_duplicados = []

    #verificar duplicados en cada columna 

    for c in csv.columns:
        #contar valores unicos en cada columna
        unicos = csv.select(c).distinct().count()
    
        #si el numero de valores unicos es menor que el total de filas, hay duplicados en esa columna
        if unicos < total_filas:
            columnas_con_duplicados.append(c)
        
    #mostrar columnas duplicadas

    print(f"Las columnas con duplicados son: {', '.join(columnas_con_duplicados)}\n")
    
    
    print('____________ NULOS EN CADA COLUMNA ______________ \n')
    
    
    null_columns_counts = list()
    numRows = csv.count()
    
    for k in csv.columns:
        # Imprimir la columna que se está evaluando
        print(f"Procesando columna: {k}")
        
        # Verificar el tipo de la columna para aplicar isnan solo a numéricos
        column_type = dict(csv.dtypes)[k]
        print(f"Tipo de dato de {k}: {column_type}")
        
        if column_type in ['int', 'double', 'float']:
            nullRows = csv.filter(
                col(k).isNull() | isnan(col(k))
            ).count()
        elif column_type == 'string':
            nullRows = csv.filter(
                col(k).isNull() | (col(k) == "") | (length(col(k)) == 0)
            ).count()
        else:
            nullRows = csv.filter(col(k).isNull()).count()
        
        nonNullRows = numRows - nullRows  # Cálculo de valores no nulos
        
        print(f"Nulos en {k}: {nullRows}, No nulos en {k}: {nonNullRows}")  # Imprimir resultados
        
        if nullRows > 0:
            temp = (k, nullRows, (nullRows / numRows) * 100)
            null_columns_counts.append(temp)
    
    print(f"{null_columns_counts}\n") 
    
# %%
def eliminar_duplicados(csv):
    
    print('____________ ELIMINAR DUPLICADOS ______________ \n')
    # Contar el número total de filas antes de eliminar duplicados
    total_filas_antes = csv.count()

    #eliminar duplicados

    csv_sin_duplicados = csv.dropDuplicates()

    # Contar el número de filas después de eliminar duplicados
    total_filas_despues = csv_sin_duplicados.count()

    # Mostrar la diferencia
    if total_filas_antes > total_filas_despues:
        print(f"Duplicados eliminados. Cantidad de filas antes: {total_filas_antes}, cantidad de filas después: {total_filas_despues}")
    else:
        print("No se encontraron duplicados para eliminar.")  
        
    return csv_sin_duplicados

#%%
def corregir_rating(app: DataFrame):
    print('_____________ LIMPIAR LOS REGISTROS EN LA COLUMNA RATING  ____________\n')
    
    app = app.withColumn(
        'Rating', 
        when(col('Rating') == ' navigation', None)  # Reemplazar 'navigation' con None (equivalente a NaN)
        .when(col('Rating') == 'Body', None)       # Reemplazar 'Body' con None
        .when(col('Rating') == '19', '1.9')        # Reemplazar '19' con '1.9'
        .when(col('Rating') == 'NaN', None)        # Reemplazar 'NaN' con None
        .otherwise(col('Rating'))                  # Mantener los demás valores iguales
    )
    
    return app
#%%

def corregir_reviews(app: DataFrame):
    
    print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA REVIEWS Y CAMBIAR EL DTYPE A DOUBLE ____________\n')

    # convertir a número y manejar errores
    app = app.withColumn(
        'Reviews',
        when(col('Reviews').cast('double').isNotNull(), col('Reviews').cast('double'))
        .otherwise(None)  # Reemplaza valores no numéricos con None
    )

    # Eliminar filas con valores None en 'Reviews'
    app = app.filter(col('Reviews').isNotNull())
    
    return app

#%%

def corregir_size(app: DataFrame):

    print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA SIZE Y CAMBIAR EL DTYPE A LONG ____________\n')


    app = app.withColumn('Size', 
        when(col('Size').endswith('M'), 
            (trim(regexp_replace(col('Size'), 'M', '')).cast('double') * 1000000).cast('long'))  # Convertir M a bytes
        .when(col('Size').endswith('K'), 
            (trim(regexp_replace(col('Size'), 'K', '')).cast('double') * 1000).cast('long'))  # Convertir K a bytes
        .when(col('Size').endswith('k'), 
            (trim(regexp_replace(col('Size'), 'k', '')).cast('double') * 1000).cast("long"))  # Convertir k a bytes
        .otherwise(None)  # Para manejar otros casos o datos no válidos
    )

    return app


#%%

def corregir_installs(app: DataFrame): 
    print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA INSTALLS Y CAMBIAR EL DTYPE A LONG ____________\n')

    app = app.withColumn('Installs', 
        when(col('Installs') == 'Free', None)  # Reemplazar "Free" con None
        .otherwise(
            regexp_replace(regexp_replace(col('Installs'), ',', ''), r'\+', '')  # Quitar comas y signo "+"
        ).cast('int'))  # Convertir a entero

    return app
# %%

def corregir_price(app: DataFrame):
    print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA PRICE Y CAMBIAR EL DTYPE A DOUBLE ____________\n')

    # Quitar el signo $ de la columna 'Price'
    app = app.withColumn('Price', regexp_replace(col('Price'), '\\$', ''))

    # Reemplazar registros que contengan 'M', 'Varies with device' y 'Everyone' con None
    app = app.withColumn('Price', 
                         when(col('Price').contains('M'), None)  # Reemplazar 'M' con None
                         .when(col('Price') == 'Varies with device', None)  # Reemplazar 'Varies with device' con None
                         .when(col('Price') == 'Everyone', None)  # Reemplazar 'Everyone' con None
                         .otherwise(col('Price')))

    # Convertir la columna 'Price' a tipo numérico (double)
    app = app.withColumn('Price', col('Price').cast('double'))

    print("10 valores aleatorios de 'Price' después de la transformación:")
    app.select('Price').orderBy(rand()).limit(10).show()
    
    return app

#%%

def calcular_distribucion_ratings(app: DataFrame):
    
    # Seleccionar la columna 'Rating' y convertirla a un DataFrame de pandas
    df_ratings = app.select('Rating').toPandas()

    # Convertir 'Rating' a tipo numérico, forzando errores a NaN y eliminándolos
    df_ratings['Rating'] = pd.to_numeric(df_ratings['Rating'], errors='coerce')
    df_ratings = df_ratings.dropna(subset=['Rating'])

    # Definir los intervalos personalizados y las etiquetas
    bins = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    labels = ['1.0 - 1.5', '1.6 - 2.0', '2.1 - 2.5', '2.6 - 3.0', '3.1 - 3.5', '3.6 - 4.0', '4.1 - 4.5', '4.6 - 5.0']

    # Agrupar los datos en los intervalos definidos
    df_ratings['RatingGroup'] = pd.cut(df_ratings['Rating'], bins=bins, labels=labels)
    rating_counts = df_ratings['RatingGroup'].value_counts().sort_index()
    
    return rating_counts

#%% 

def graficar_distribucion_ratings(app: DataFrame):

    rating_counts = calcular_distribucion_ratings(app)
    
    # Configurar el tamaño del gráfico
    plt.figure(figsize=[10, 6])

    # Crear el histograma de ratings
    rating_counts.plot(kind='bar', color='skyblue')
    plt.title('Distribución de ratings')
    plt.xlabel('Intervalo de ratings')
    plt.ylabel('Frecuencia')
    plt.xticks(rotation=45)
    plt.show()

    print('En este gráfico se puede notar claramente que la mayor cantidad de votos de rating se sitúa en el rango de 4.1 a 4.5')

#%%

#Seleccionar columnas relevantes
#covnertir a pandas para poder graficar

def matriz_correlacion(app: DataFrame):
    
    print('_____________ Matriz de correlación ____________\n')
    
    app_grafico = app.select('Rating', 'Reviews', 'Size').toPandas()


    #Calcular matriz de correlación

    correlacion = app_grafico[['Rating', 'Reviews','Size']].corr()

    #Visualizar la matriz de correlación

    plt.figure(figsize=(8,6))
    sns.heatmap(correlacion, annot = True, cmap= 'coolwarm', fmt = ".2f")
    plt.title("Matriz de correlación entre Rating, Reviews, Size")
    plt.show()
    
    print("En el gráfico se puede apreciar que el comportamiento de las variables no tienen relación entre ellas")
#%%
def agg_categoria (df, group_column = 'Category', column_installs = 'Installs', column_rating = 'rating' ):
    
    print('_____________ Agrupar para graficar las columnas que me interesan analizar y convertir a df para poder graficar ____________\n')
    
    app_agg = df.groupBy(group_column).agg(
        F.sum(column_installs).alias('total_installs'),
        F.avg(column_rating).alias('rating_promedio')
    )
    
    #convertir a df graficar
    pd_app_agg = app_agg.toPandas()
    
    return pd_app_agg

#%% 
def grafico_install_category(pd_df,column_installs = 'total_installs', category_column = 'Category'):
    plt.figure(figsize = (14,20))
    
    #grafico
    
    plt.subplot(1,2,1)
    sns.barplot(
        data = pd_df.sort_values(column_installs, ascending = False),
        x = column_installs,
        y = category_column,
        palette = 'viridis'
    )
    plt.title('Categoría con más installs')
    plt.xlabel('Total de installs')
    plt.ylabel(category_column)
    
    plt.show()
#%%
def plot_categoria_promedio(pd_df, column_rating = 'rating_promedio', column_category = 'Category'):
    plt.figure(figsize = (20,15))
    
    #grafico
    plt.subplot(1,2,2)
    sns.barplot(
        data = pd_df.sort_values(column_rating, ascending = False),
        x = column_rating,
        y = column_category,
        palette = 'viridis'
    )
    plt.title('Categoría con mejor rating promedio')
    plt.xlabel('Promedio de rating')
    plt.ylabel(column_category)
    
    plt.show()
#%%
def agg_installs_rating(df, column_install = 'Installs', column_rating = 'Rating', top_n = 5):
    mas_installs = df.orderBy(F.desc(column_install)).limit(top_n)
    mas_rating= df.orderBy(F.desc(column_rating)).limit(top_n)
    
    #convertir a df para graficar
    pd_installs = mas_installs.toPandas()
    pd_rating = mas_rating.toPandas()
    
    return pd_installs, pd_rating
#%%

def app_mas_installs(pd_df, app_column = 'App', column_install = 'Installs'):
    plt.figure(figsize=(12,6))
    sns.barplot(
        data = pd_df,
        x = app_column,
        y = column_install,
        palette = 'viridis'
    )
    plt.title('Top 5 apps más instaladas')
    plt.xlabel('App')
    plt.ylabel('Total de installs')
    plt.show()

#%%
def top_app_rating(pd_df, column_rating = 'Rating', app_column = 'App'):
    plt.figure(figsize=(12,6))
    sns.barplot(
        data = pd_df,
        x = column_rating,
        y = app_column,
        palette = 'viridis'
    )    
    plt.title('Top 5 apps con mejor rating')
    plt.xlabel('Rating promedio')
    plt.ylabel('Aplicación')
    plt.show()
    
def conclusion_final():
    print('Con todo lo analizado hasta ahora, se puede llegar a la conclusión que los ratings de \
        las apps se concentran entre el puntaje 4.1 a 4.5, la categoria on mejor rating son las de \
        eventos, dentro de esta categoría se encuentran las apps para organizar eventos o tarjetas \
        de invitación a los mismos. Por otro lado, las 5 apps con mejor rating (todas con un puntaje de 5) \
        pertenecen a diferentes categorías. Las categorías con mejor rating. \
        Por último, las 5 apps más instaladas son google photos, hangouts, gmail app, google + \
        y google drive, todas pertenecen a la categoría communication.')