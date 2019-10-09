import os
import pandas

from src.clases import Constantes
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
        #dataframe = dataframe.replace('[^a-zA-Z0-9 ]', '', regex=True)
        #dataframe = dataframe.apply(lambda x: x.str.replace('[^a-zA-Z0-9]', ''), axis=0)

        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect(Constantes.CASSANDRA_KEYSPACE, wait_for_all_pools=True)
        session.execute("TRUNCATE PuntosDeCarga")

        query = "INSERT INTO PuntosDeCarga(id, entidad, modalidad, cuit, latitud, longitud, nombre_ubicacion, " \
                "tipo_ubicacion, calle, numero, barrio, comuna, partido, localidad, provincia, codigo_postal) VALUES" \
                " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        prepared = session.prepare(query)
        i = 1
        for item in dataframe.rdd.collect():
            session.execute(prepared, (i, item[Constantes.COL_ENTIDAD], item[Constantes.COL_MODALIDAD],
                                       item[Constantes.COL_CUIT], item[Constantes.COL_LATITUD],
                                       item[Constantes.COL_LONGITUD], item[Constantes.COL_UBICACION],
                                       item[Constantes.COL_TIPOUBICACION], item[Constantes.COL_CALLE],
                                       item[Constantes.COL_NUMERO], item[Constantes.COL_BARRIO],
                                       item[Constantes.COL_COMUNA], item[Constantes.COL_PARTIDO],
                                       item[Constantes.COL_LOCALIDAD], item[Constantes.COL_PROVINCIA],
                                       item[Constantes.COL_CODIGOPOSTAL]))
            i = i + 1

        return i
