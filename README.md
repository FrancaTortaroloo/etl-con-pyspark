## Hola! En el siguiente proyecto quiero poner en práctica PySpark, la interfaz de Apache Spark en Python. 🤓

### Para ello he escogido un csv sobre aplicaciones de la app store de google que contiene la siguiente información

📍 App (nombre de app) \
📍 Category\
📍 Rating\
📍 Size\
📍 Reviews\
📍 Installs (cantidad)\
📍 Type\
📍 Price\
📍 Content\
📍 Rating\
📍 Genre\
📍 Last updated


Por ahora lo único que he hecho es un análsis exploratorio del csv con pyspark, lo iré completando cada vez un poquito más al proyecto e iré compartiendo avances. 😀

Dentro de la carpeta 📂 `input data` pueden encontrar el csv de de base, sin realizar cambios.
 
Dentro de la carpeta 📂 `scripts` se encuentran 2 archivos python, Dentro del archivo `apps_pyspark.py` se procesan los datos descargados y genera archivos limpios y transformados.

### Conclusión final

Con todo lo analizado hasta ahora, se puede llegar a la conclusión que los ratings de las apps se concentran entre el puntaje 4.1 a 4.5.

![Ratings](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/rating.png)


La categoria con mejor promedio de rating son las de eventos, dentro de esta categoría se encuentran las apps para organizar eventos o tarjetas de invitación a los mismos. 

![Categoría con mejor promedio de rating](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/categor%C3%ADa%20rating%20promedio.png)

Por otro lado, estas son las 5 apps con mejor rating (todas con un puntaje de 5) y pertenecen a diferentes categorías.

![Apps con mejor rating](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/top%205%20apps%20con%20mejor%20rating.png)

![Apps y sus Categorías](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/App%20-%20categor%C3%ADa%20-%20rating.png)

Por último, las 5 apps más instaladas son google photos, hangouts, gmail app, google + y google drive, todas pertenecen a la categoría communication.

![Apps más instaladas](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/top%205%20apps%20m%C3%A1s%20instaladas.png)


Para finalizar, viendo el mapa de correlación y todos estos resultados, se puede intuir que las variables no tienen relación entre sí, no dependen entre ellas, por lo tanno se puede notar un patrón de comportamiento.

![Heatmap](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/heatmap.png)