#%%importamos librerias que necesitamos para la lectura, conversión y limpieza de datos

import pyspark
from pyspark.sql import SparkSession


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
'''
#cambiar los dtypes de las columnas


def create_data_schema(column_definitions):
    data_schema = []
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "date": DateType()
    }
    
    for column_name, column_type in column_definitions:
        data_schema.append(StructField(column_name, type_mapping.get(column_type, StringType()), True))
    
    return StructType(fields=data_schema)
'''

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

#%%


#%%
'''
