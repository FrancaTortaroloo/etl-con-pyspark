#%%importamos librerias que necesitamos para la lectura, conversión y limpieza de datos

import pyspark
from pyspark.sql import SparkSession,DataFrame


from pyspark.sql.types import *
from pyspark.sql.functions import *


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
