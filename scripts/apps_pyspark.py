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
    ("Size", "integer"),
    ("Installs", "integer"),
    ("Type", "string"),
    ("Price", "integer"),
    ("Content Rating", "string"),
    ("Genres", "string"),
    ("Last Updated", "date"), 
    ("Current Ver", "date"),
    ("Android Ver", "date"),
    
]

ruta = "/Users/franca/Documents/GitHub/ETL-con-pyspark/input_data/googleplaystore.csv"  
app_df = func.read_csv_with_schema(spark, ruta, column_definitions)
'''
# %%

explorar = func.exploracion_datos(app)

# %%

columnas_eliminar = ['Current Ver', 'Android Ver']

sin_columnas = func.eliminar_columnas(app, columnas_eliminar)

#%%


#%%



#%%
guardar_csv = func.exportar_csv("/Users/franca/Documents/GitHub/ETL-con-pyspark/output_data")
# %%
df_pandas = app_df.toPandas()

#%%
