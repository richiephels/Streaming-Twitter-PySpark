# coding=utf-8
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# crear configuración spark
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# crear instancia de  spark con la configuración anterior
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# creamos el Streaming Context desde el contexto de spark anterior con el tamaño de ventana 2 segundos
ssc = StreamingContext(sc, 2)
# establecemos un punto de control para permitir la recuperación de RDD
ssc.checkpoint("checkpoint_TwitterApp")
# leer datos del puerto 9009
dataStream = ssc.socketTextStream("localhost",9009)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    # extraer los hashtags del data frame y convertirlos en array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extraer los recuentos del data frame y convertirlos en array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    # inicializar y enviar los datos a través de la API REST
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # obgener el spark sql singleton context del contexto actual
        sql_context = get_sql_context_instance(rdd.context)
        # convertir el RDD en Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # crear un DF desde el Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Registre el marco de datos como tabla
        hashtags_df.registerTempTable("hashtags")
        # obtener los 10 hashtags principales de la tabla usando SQL e imprimirlos
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        # llamada al método para preparar los 10 hashtags DF y enviarlos
        send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# dividir cada tweet en palabras
words = dataStream.flatMap(lambda line: line.split(" "))
# filtrar las palabras para obtener solo hashtags, luego asignar cada hashtag para que sea un par de (hashtag, 1)
#hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
hashtags = words.map(lambda x: (x, 1))
# agregando el conteo de cada hashtag a su último conteo
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# hacer el procesamiento para cada RDD generado en cada intervalo
tags_totals.foreachRDD(process_rdd)

# iniciar la transmisión
ssc.start()
# esperar a que la transmisión termine
ssc.awaitTermination()