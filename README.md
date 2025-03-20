## Hello! In this project, I want to put PySpark into practice, the Apache Spark interface for Python. 🤓

### To do this, I have chosen a CSV file about applications from the Google Play Store, which contains the following information:

📍 App (app name) \
📍 Category\
📍 Rating\
📍 Size\
📍 Reviews\
📍 Installs (number)\
📍 Type\
📍 Price\
📍 Content Rating\
📍 Genre\
📍 Last updated\

So far, I have only performed an exploratory analysis of the CSV using PySpark. I will keep adding more to the project and sharing updates. 😀

Inside the 📂 input data folder, you can find the original CSV file without any modifications.

Inside the 📂 scripts folder, there are two Python files. The apps_pyspark.py file processes the downloaded data and generates clean and transformed files.

Final Conclusion
Based on the analysis so far, it can be concluded that app ratings are mostly concentrated between scores of 4.1 and 4.5.

![Ratings](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/rating.png)

The category with the highest average rating is "Events," which includes apps for organizing events or creating invitation cards.

![Categoría con mejor promedio de rating](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/categor%C3%ADa%20rating%20promedio.png)

On the other hand, these are the top 5 highest-rated apps (all with a score of 5), belonging to different categories.

![Apps con mejor rating](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/top%205%20apps%20con%20mejor%20rating.png)

![Apps y sus Categorías](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/App%20-%20categor%C3%ADa%20-%20rating.png)

Lastly, the 5 most installed apps are Google Photos, Hangouts, Gmail App, Google+, and Google Drive, all of which belong to the "Communication" category.

![Apps más instaladas](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/top%205%20apps%20m%C3%A1s%20instaladas.png)

To conclude, by analyzing the correlation heatmap and these results, it can be inferred that the variables do not have a significant relationship with each other. They are independent, and therefore, no clear behavioral pattern can be observed.

![Heatmap](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/heatmap.png)

---------------------------------------------------------------

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


Para finalizar, viendo el mapa de correlación y todos estos resultados, se puede intuir que las variables no tienen relación entre sí, no dependen entre ellas, por lo tanto no se puede notar un patrón de comportamiento.

![Heatmap](https://github.com/FrancaTortaroloo/etl-con-pyspark/blob/main/assets/heatmap.png)
