#%% 
import pyspark
from pyspark.sql import SparkSession
import functions_pyspark as func
from pyspark.sql.types import *
from pyspark.sql.functions import *



#%%

visualizacion = func.leer_archivo("/Users/franca/Documents/GitHub/ETL-con-pyspark/googleplaystore.csv") 
print(visualizacion)
# %%

#imprimir el schema

visualizacion.printSchema()
# %%

explorar = func.exploracion_datos(visualizacion)

# %%

sin_duplicados = func.eliminar_duplicados(visualizacion)
# %%

columnas_eliminar = ['Current Ver', 'Android Ver']

sin_columnas = func.eliminar_columnas(visualizacion, columnas_eliminar)


# %%
# cambiar dtypes y comprobar que el cambio se haya hecho

if __name__ == "__main__":
    # Inicializa una sesión de Spark
    spark = SparkSession.builder.appName("Ejemplo").getOrCreate() 
    

column_definitions = [
    ("App", "string"),
    ("Category", "string"),
    ("Rating", "integer"),
    ("Reviews", "integer"),
    ("Size", "integer"),
    ("Installs", "integer"),
    ("Type", "string"),
    ("Price", "integer"),
    ("Content", "string"),
    ("Genres", "string"),
    ("Last Updated", "date")
]

ruta = "/Users/franca/Documents/GitHub/ETL-con-pyspark/googleplaystore.csv"  
app_df = func.read_csv_with_schema(spark, ruta, column_definitions)

#%%
