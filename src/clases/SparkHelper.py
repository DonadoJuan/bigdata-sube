import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from cassandra.cluster import Cluster

class SparkHelper:

    def __init__(self):
        self.conf = SparkConf().set("spark.driver.memory", "10g") \
                               .set("spark.debug.maxToStringFields", "10000") \
                               .set("spark.driver.maxResultSize", "1g") \
                               .setMaster("local[*]") \
                               .setAppName("processCSV")
        self.context = SparkContext(conf=self.conf)
        self.context.setLogLevel('ERROR')
        self.spark = SQLContext(self.context)

    def readcsv(self, ruta, showheader=True):
        """
        Lee un archivo CSV con Spark y retorna un dataframe.
        :param ruta: Ruta completa al archivo a leer.
        :param showheader: Indica si se debe incluir el encabezado al leer el CSV.
        :return: Objeto dataframe con los datos leídos.
        """
        df = self.spark.read.csv(ruta, header=showheader)
        df = self.renamecolumns(df)
        return df

    def readparquet(self, rutahdfs):
        """
        Lee un archivo en formato Parquet del HDFS y retorna un Dataframe.
        :param rutahdfs: Ruta del HDFS al archivo.
        :return: Objeto dataframe que representa al archivo.
        """
        return self.spark.read.parquet(rutahdfs)

    def renamecolumns(self, dataframe):
        """
        Renombra las columnas que vienen con caracteres inválidos en el header.
        :param dataframe: Objeto dataframe que contiene los datos.
        :return: Dataframe con las columnas renombradas.
        """
        return dataframe.withColumnRenamed("Id Entidad", "IdEntidad") \
                        .withColumnRenamed("Id Ubicaci", "IdUbicacion") \
                        .withColumnRenamed("Tipo Ubica", "TipoUbicacion")

    def writetoparquet(self, dataframe, ruta, archivo):
        """
        Escribe el dataframe pasado por parámetro en el HDFS en formato Parquet.
        :param dataframe: Objeto dataframe a escribir.
        :param ruta: Directorio en donde se guardará el archivo.
        :param archivo: Nombre del archivo en formato Parquet.
        """
        rutacompleta = os.path.join(ruta, archivo)
        dataframe.write.format('parquet').mode("overwrite").save(rutacompleta)

    def writetocassandra(self, dataframe):
        """
        Escribe el dataframe en la base de datos Cassandra.
        :param dataframe: Objeto dataframe a escribir.
        """


    # def writetoavro(self, dataframe, ruta, archivo):
    #     """
    #     Escribe el dataframe pasado por parámetro en el HDFS en formato AVRO.
    #     :param dataframe: Objeto dataframe a escribir.
    #     :param ruta: Directorio en donde se guardará el archivo.
    #     :param archivo: Nombre del archivo en formato AVRO.
    #     """
    #     rutacompleta = ruta + "/" + archivo
    #     dataframe.write.format("com.databricks.spark.avro").mode('overwrite').save(rutacompleta)
