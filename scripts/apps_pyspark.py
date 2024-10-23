#%% 
import pyspark
from pyspark.sql import SparkSession
import functions_pyspark as func
from pyspark.sql.types import *
from pyspark.sql.functions import *
import seaborn as sns
import matplotlib.pyplot as plt


#%%

app = func.leer_archivo("/Users/franca/Documents/GitHub/ETL-con-pyspark/input_data/googleplaystore.csv") 
print(app)
# %%

#imprimir el schema

app.printSchema()


# %%

explorar = func.exploracion_datos(app)

# %%
# Definir los reemplazos
app = app.withColumn(
    'Rating', 
    when(col('Rating') == ' navigation', None)  # Reemplazar 'navigation' con None (equivalente a NaN)
    .when(col('Rating') == 'Body', None)       # Reemplazar 'Body' con None
    .when(col('Rating') == '19', '1.9')        # Reemplazar '19' con '1.9'
    .when(col('Rating') == 'NaN', None)        # Reemplazar 'NaN' con None
    .otherwise(col('Rating'))                  # Mantener los demás valores iguales
)
#%%
#asegurarme que la columna reviews solo tenga datos numericos

# convertir a número y manejar errores
app = app.withColumn(
    'Reviews',
    when(col('Reviews').cast('double').isNotNull(), col('Reviews').cast('double'))
    .otherwise(None)  # Reemplaza valores no numéricos con None
)

# Eliminar filas con valores None en 'Reviews'
app = app.filter(col('Reviews').isNotNull())

#mostrar el DF limpio
app.show()

#%%
#convertir los datos de la columna size

app = app.withColumn('Size_Numeric', 
                     when(col('Size').endswith('M'), 
                          trim(regexp_replace(col('Size'), 'M', '')) * 1000000)  # Convertir M a bytes
                     ).when(col('Size').endswith('G'), 
                          trim(regexp_replace(col('Size'), 'G', '')) * 1000000000)  # Convertir G a bytes
                     .otherwise(None)  # Para manejar otros casos o datos no válidos

# Mostrar el DataFrame para verificar el cambio
app.select('Size', 'Size_Numeric').show()



# %%
# cambiar dtypes y comprobar que el cambio se haya hecho
'''
if __name__ == "__main__":
    # Inicializa una sesión de Spark
    spark = SparkSession.builder.appName("Ejemplo").getOrCreate() 
    

column_definitions = [
    ("App", "string"),
    ("Category", "string"),
    ("Rating", "integer"),
    ("Reviews", "integer"),
    ("Size", "string"),
    ("Installs", "string"),
    ("Type", "string"),
    ("Price", "integer"),
    ("Content Rating", "string"),
    ("Genres", "string"),
    ("Last Updated", "date"), 
    ("Current Ver", "string"),
    ("Android Ver", "string"),
    
]

ruta = "/Users/franca/Documents/GitHub/ETL-con-pyspark/input_data/googleplaystore.csv"  
app_df = func.read_csv_with_schema(spark, ruta, column_definitions)
'''


#%%


#%%

#%%
