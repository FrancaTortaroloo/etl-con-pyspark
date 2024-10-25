#%% 
import pyspark
from pyspark.sql import SparkSession
import functions_pyspark as func
from pyspark.sql.types import *
from pyspark.sql.functions import *
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

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

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA RATING Y CAMBIAR EL DTYPE A DOUBLE ____________\n')


app = app.withColumn(
    'Rating', 
    when(col('Rating') == ' navigation', None)  # Reemplazar 'navigation' con None (equivalente a NaN)
    .when(col('Rating') == 'Body', None)       # Reemplazar 'Body' con None
    .when(col('Rating') == '19', '1.9')        # Reemplazar '19' con '1.9'
    .when(col('Rating') == 'NaN', None)        # Reemplazar 'NaN' con None
    .otherwise(col('Rating'))                  # Mantener los demás valores iguales
)
#%%
print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA REVIEWS Y CAMBIAR EL DTYPE A DOUBLE ____________\n')

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

# Mostrar el DataFrame para verificar el cambio
app.select('Size').show()


#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA INSTALLS Y CAMBIAR EL DTYPE A LONG ____________\n')

app = app.withColumn('Installs', 
                     when(col('Installs') == 'Free', None)  # Reemplazar "Free" con None
                     .otherwise(
                         regexp_replace(regexp_replace(col('Installs'), ',', ''), r'\+', '')  # Quitar comas y signo "+"
                     ).cast('int'))  # Convertir a entero

# Mostrar los resultados para verificar
app.select('Installs').show()

#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA TYPE  ____________\n')

app = app.fillna({'Type': 'Unknown'})

# Mostrar el DataFrame para verificar el cambio
app.select('Type').show()

#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA PRICE Y CAMBIAR EL DTYPE A DOUBLE ____________\n')

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

print('_____________ ANÁLISIS DE LA DISTRIBUCIÓN DE RATINGS____________\n')
print('_____________ Histograma para ver la distribución de valores específicos ____________\n')

#seleccionar la columna a graficar y convertir a pandas

df_ratings = app.select('Rating').toPandas()

#convertir rating a numerico, forzando errores a NaN y eliminarlos

df_ratings['Rating'] = pd.to_numeric(df_ratings['Rating'], errors = 'coerce')
df_ratings = df_ratings.dropna(subset=['Rating'])

#definir los intervalos personalizados y etiquetas

bins = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
labels = ['1.0 - 1.5', '1.6 - 2.0', '2.1 - 2.5', '2.6 - 3.0', '3.1 - 3.5', '3.6 - 4', '4.1 - 4.5', '4.6 - 5.0' ]

# Agruparlos en los intervalos definidos 

df_ratings['RatingGroup'] = pd.cut(df_ratings['Rating'], bins = bins, labels = labels)
rating_counts = df_ratings['RatingGroup'].value_counts().sort_index()

#Configurar el tamaño de los gráficos

plt.figure(figsize=[10, 6])

#Histograma de ratingss

rating_counts.plot(kind = 'bar', color = 'skyblue')
plt.title('Distribución de ratings')
plt.xlabel('Intervalo de ratings')
plt.ylabel('Frecuencia')
plt.xticks(rotation = 45)
plt.show()
#%%
print('_____________ Tendencia de concentración de los datos ____________\n')

plt.figure(figsize = (10,6))
sns.barplot(x = rating_counts.index, y = rating_counts.values, palette = 'Blues_d')
plt.title('Distribución de Rating')
plt.xlabel('Intervalo de rating')
plt.ylabel('Frecuencia')
plt.xticks(rotation=45)
plt.show()

#%%

#Seleccionar columnas relevantes
#covnertir a pandas para poder graficar

app_grafico = app.select('Rating', 'Reviews', 'Size').toPandas()


#Calcular matriz de correlación

matriz_correlacion = app_grafico[['Rating', 'Reviews','Size']].corr()

#Visualizar la matriz de correlación

plt.figure(figsize=(8,6))
sns.heatmap(matriz_correlacion, annot = True, cmap= 'coolwarm', fmt = ".2f")
plt.title("Matriz de correlación entre Rating, Reviews, Size")
plt.show()

#%%