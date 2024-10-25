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

#%%
app = func.eliminar_duplicados(app)
# %%

app = func.corregir_rating(app)

#%%

app = func.corregir_reviews(app)

#%%

app = func.corregir_size(app)


#%%

app = func.corregir_installs(app)

#%%

print('_____________ CORREGIR LOS REGISTROS EN LA COLUMNA TYPE  ____________\n')

app = app.fillna({'Type': 'Unknown'})


#%%

print("Valores de 'Price' antes de la transformación:")
app.select('Price').show(10)

app = func.corregir_price(app)



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