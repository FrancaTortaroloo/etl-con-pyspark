#%% 
import pyspark
from pyspark.sql import SparkSession
import functions_pyspark as func
from pyspark.sql.types import *
from pyspark.sql.functions import *

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
app = func.corregir_price(app)
#%%
app.printSchema()
# %%
#describir los datos estadisticos
app['Rating', 'Reviews', 'Size', 'Installs', 'Price'].describe().toPandas()
#%%
print('_____________ ANÁLISIS DE LA DISTRIBUCIÓN DE RATINGS ____________\n')
print('_____________ Histograma para ver la distribución de valores específicos ____________\n')
calcular_distribucion = func.calcular_distribucion_ratings(app)
graficar = func.graficar_distribucion_ratings(app)
#%%
matriz = func.matriz_correlacion(app)
#%%
print('_____________ Concatenar para usar las columnas que quiero analizar y graficar ____________\n')

concat = app.select(app.App,
                    app.Category,
                    app.Rating,
                    app.Installs)

concat.show(truncate = False)

#%%
pd_app_agg = func.agg_categoria(concat)
#%%
plot = func.grafico_install_category(pd_app_agg)
#%%
plot_avgrating = func.plot_categoria_promedio(pd_app_agg)
#%%
pd_installs, pd_rating = func.agg_installs_rating(concat)
#%%
top_apps = func.app_mas_installs(pd_installs)
#%%
top_rating = func.top_app_rating(pd_rating)
#%%
pd_rating[['App', 'Rating','Category']]
#%%