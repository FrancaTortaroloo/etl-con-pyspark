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
    app.show(5, truncate = True)
    return app
    
#%%

def exploracion_datos(csv):
    print('_____________ INFORMACIÓN GENERAL DEL CSV ____________\n')

    print(f"El número de filas que tenemos es de {csv.count()}.\nEl número de columnas es de {len(csv.columns)}\n")

    print('_____________ VALORES NULOS DE CADA COLUMNA ______________\n')
    
    #app.columns accede a la lista de columnas del df
    # for c in app.columnas: accede a cada columna del df
    #funcion col: referirnos a cada columna por su nombre, devuelve true o false, con cast int convierte esos true en 1 y false en 0. Los nulos serian true.  
    #sum para sumar los valores generados por los nulos de cada columna y se suman para verificar la cantidad
    
    nulos = csv.select([sum(col(c).isNull().cast("int")).alias(c) for c in csv.columns])
    print(nulos.show())

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

    print(f"Las columnas con duplicados son: {', '.join(columnas_con_duplicados)}")

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
def eliminar_columnas(csv,columns):
    csv_limpio = csv.drop(*columns)
    #verificar que se eliminaron las columnas

    print(csv_limpio.limit(4).toPandas())
    
#%%
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

def read_csv_with_schema(spark, path, column_definitions):
    # Crear el esquema de datos
    final_struc = create_data_schema(column_definitions)
    
    # Leer el archivo CSV
    app = spark.read.csv(path=path, schema=final_struc, header=True)
    
    # Imprimir el esquema
    app.printSchema()
    
    return app

#%%
