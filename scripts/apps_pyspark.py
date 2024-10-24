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

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA RATING Y CAMBIAR EL DTYPE A DOUBLE____________\n')


app = app.withColumn(
    'Rating', 
    when(col('Rating') == ' navigation', None)  # Reemplazar 'navigation' con None (equivalente a NaN)
    .when(col('Rating') == 'Body', None)       # Reemplazar 'Body' con None
    .when(col('Rating') == '19', '1.9')        # Reemplazar '19' con '1.9'
    .when(col('Rating') == 'NaN', None)        # Reemplazar 'NaN' con None
    .otherwise(col('Rating'))                  # Mantener los demás valores iguales
)
#%%
print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA REVIEWS Y CAMBIAR EL DTYPE A DOUBLE____________\n')

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

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA SIZE Y CAMBIAR EL DTYPE A LONG____________\n')


app = app.withColumn('Size', 
    when(col('Size').endswith('M'), 
         (trim(regexp_replace(col('Size'), 'M', '')).cast('double') * 1000000).cast('long'))  # Convertir M a bytes
    .when(col('Size').endswith('K'), 
         (trim(regexp_replace(col('Size'), 'K', '')).cast('double') * 1000).cast('long'))  # Convertir K a bytes
    .when(col('Size').endswith('k'), 
         (trim(regexp_replace(col('Size'), 'k', '')).cast('double') * 1000).cast("long"))  # Convertir k a bytes
    .otherwise(None)  # Para manejar otros casos o datos no válidos
)

# Mostrar el DataFrame para verificar el cambio
app.select('Size').show()


#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA INSTALLS Y CAMBIAR EL DTYPE A LONG____________\n')

app = app.withColumn('Installs', 
                     when(col('Installs') == 'Free', None)  # Reemplazar "Free" con None
                     .otherwise(
                         regexp_replace(regexp_replace(col('Installs'), ',', ''), r'\+', '')  # Quitar comas y signo "+"
                     ).cast('int'))  # Convertir a entero

# Mostrar los resultados para verificar
app.select('Installs').show()

#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA TYPE____________\n')

app = app.fillna({'Type': 'Unknown'})

# Mostrar el DataFrame para verificar el cambio
app.select('Type').show()

#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA PRICE Y CAMBIAR EL DTYPE A DOUBLE____________\n')

app = app.withColumn('Price', 
                     regexp_replace(col('Price'), '\\$', '')  # Quitar el signo $
                     )

# Reemplazar registros que contengan 'M' o sean 'Varies with device' y 'Everyone' con None
app = app.withColumn('Price', 
                     when(col('Price').contains('M'), None)  # Reemplazar registros con 'M' con None
                     .when(col('Price') == 'Varies with device', None)  # Reemplazar 'Varies with device' con None
                     .when(col('Price') == 'Everyone', None)  # Reemplazar 'Everyone' con None
                     .otherwise(col('Price')))

# Convertir la columna 'Price' a tipo numérico (double)
app = app.withColumn('Price', col('Price').cast('double'))

# Mostrar el DataFrame para verificar los cambios
app.select('Price').show()


#%%
app.printSchema()

# %%

#describir los datos estadisticos

app['Rating', 'Reviews', 'Size', 'Installs', 'Price'].describe().toPandas()

#%%


#%%

#%%
