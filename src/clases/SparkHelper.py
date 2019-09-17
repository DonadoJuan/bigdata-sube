from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

class SparkHelper:

    def readcsv(self, ruta, showheader=True):
        """
        Lee un archivo CSV con Spark y retorna un dataframe.
        :param ruta: Ruta completa al archivo a leer.
        :param showheader: Indica si se debe incluir el encabezado al leer el CSV.
        :return: Objeto dataframe con los datos leídos.
        """
        conf = SparkConf().set("spark.driver.memory", "10g") \
                          .set("spark.debug.maxToStringFields", "10000") \
                          .set("spark.driver.maxResultSize", "1g") \
                          .setMaster("local[*]") \
                          .setAppName("processCSV")

        sc = SparkContext(conf=conf)
        sp = SQLContext(sc)
        df = sp.read.csv(ruta, header=showheader)
        df = self.renamecolumns(df)
        return df

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
        rutacompleta = ruta + "/" + archivo
        dataframe.write.format('parquet').mode("overwrite").save(rutacompleta)

    def writetoavro(self, dataframe, ruta, archivo):
        """
        Escribe el dataframe pasado por parámetro en el HDFS en formato AVRO.
        :param dataframe: Objeto dataframe a escribir.
        :param ruta: Directorio en donde se guardará el archivo.
        :param archivo: Nombre del archivo en formato AVRO.
        """
        rutacompleta = ruta + "/" + archivo
        dataframe.write.format("com.databricks.spark.avro").mode('overwrite').save(rutacompleta)

