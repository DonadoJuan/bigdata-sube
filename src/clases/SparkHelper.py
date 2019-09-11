from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


class SparkHelper:

    def readcsv(self, ruta, showheader=True):
        conf = SparkConf().set("spark.driver.memory", "10g") \
                          .set("spark.debug.maxToStringFields", "10000") \
                          .set("spark.driver.maxResultSize", "1g") \
                          .setMaster("local[*]") \
                          .setAppName("processCSV")

        sc = SparkContext(conf=conf)
        sp = SQLContext(sc)
        df = sp.read.csv(ruta, header=showheader)
        return df

    def writetoparquet(self, dataframe, ruta, archivo):
        rutacompleta = ruta + "/" + archivo
        dataframe = self.renamecolumns(dataframe)
        dataframe.write.parquet(rutacompleta)

    def renamecolumns(self, dataframe):
        return dataframe.withColumnRenamed("Id Entidad", "IdEntidad") \
                        .withColumnRenamed("Id Ubicaci", "IdUbicacion") \
                        .withColumnRenamed("Tipo Ubica", "TipoUbicacion")

