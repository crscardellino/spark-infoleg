# Spark para InfoLEG

Repositorio de código para el artículo de Medium: [Procesando Datos con Spark (y IV) - Corriendo una aplicación con PySpark](https://medium.com/@crscardellino/procesando-datos-con-spark-y-iv-corriendo-una-aplicaci%C3%B3n-con-pyspark-5c26e828465d)

## Uso de la aplicación

    $SPARK_HOME/bin/spark-submit --master $SPARK_CLUSTER ./spark-infoleg.py \
        [-h] [--topics TOPICS] [--iterations ITERATIONS] \
            [--min-df MIN_DF] INPUT_FILE OUTPUT_FILE

    Argumentos:
      INPUT_FILE            Dirección al archivo de texto del InfoLEG.
      OUTPUT_FILE           Dirección al archivo de texto donde imprimir los
                            resultados.

    Argumentos opcionales:
      --topics TOPICS       Cantidad de tópicos para LDA.
      --iterations ITERATIONS
                            Cantidad de iteraciones máxima para LDA.
      --min-df MIN_DF       Mínima cantidad de documentos para considerar una
                            palabra.

## Ejemplo de uso

    $SPARK_HOME/bin/spark-submit --master local[*] ./spark-infoleg.py ./infoleg.txt ./topics.txt --topics 10
