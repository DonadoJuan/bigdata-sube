from pyspark.shell import spark
from pyspark.sql import SparkSession

import typing as T
import pyspark.sql.types
import cytoolz.curried as tz
import pyspark
from pyspark.sql.functions import explode
from pyspark.sql.types import BooleanType


def schema_to_columns(schema:pyspark.sql.types.StructType) -> T.List[T.List[str]]:
        columns = list()

        def helper(schm: pyspark.sql.types.StructType, prefix: list = None):

                if prefix is None:
                        prefix = list()

                for item in schm.fields:
                        if isinstance(item.dataType, pyspark.sql.types.StructType):
                                helper(item.dataType, prefix + [item.name])
                        else:
                                columns.append(prefix + [item.name])

        helper(schema)

        return columns


def flatten_array(frame: pyspark.sql.DataFrame) -> (pyspark.sql.DataFrame, BooleanType):
        have_array = False
        aliased_columns = list()
        for column, t_column in frame.dtypes:
                if t_column.startswith('array<'):
                        have_array = True
                        c = explode(frame[column]).alias(column)
                else:
                        c = tz.get_in([column], frame)
                aliased_columns.append(c)
        return (frame.select(aliased_columns), have_array)


def flatten_frame(frame: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        aliased_columns = list()

        for col_spec in schema_to_columns(frame.schema):
                c = tz.get_in(col_spec, frame)
                if len(col_spec) == 1:
                        aliased_columns.append(c)
                else:
                        aliased_columns.append(c.alias('_'.join(col_spec)))

        return frame.select(aliased_columns)


def flatten_all(frame: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        frame = flatten_frame(frame)
        (frame, have_array) = flatten_array(frame)
        if have_array:
                return flatten_all(frame)
        else:
                return frame

df = spark.read.json("source.geojson")

df.registerTempTable("sube_tmp")


#sql = spark.sql("select txid,blocktime,version,confirmations,time,locktime,vsize,size,weight,hex,blockhash,explode(vout) from sube_tmp  lateral view explode(vin)  vin_names  as vin_out")
#sql = spark.sql("select Id Entidad,Entidad,Nro.Cuit,Tipo Ubica,Modalidad,Barrio,Comuna,Partido,Provincia,Localidad,Latitud,Longitud,explode(vout) from sube_tmp  lateral view explode(vin)  vin_names  as vin_out")

#flat = flatten_all(sql)


#spark.sql(" drop table btc")

#flat.write.saveAsTable("btc",format="parquet", mode="overwrite")

#sqlserver_df = spark.read.format("jdbc").options(url="jdbc:sqlserver://eundbcadbssql01.database.windows.net:1433;database=eundbcadbssql01;user=sqladmin;password=Welcome12345*", dbtable="bitcoinPOC").option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver').load()
