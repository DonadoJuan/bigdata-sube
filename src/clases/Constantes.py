# Archivo de origen en formato CSV
SOURCE_FILE = "data/data-source.csv"
# Directorio de output en HDFS
HDFS_OUTPUT_DIR = "hdfs://kudu:20500/Grupo1/procesado/"
# Nombre del archivo procesado en formato Parquet
PARQUET_FILE = "resultado.parquet"
# Nombre del Keyspace por defecto en Cassandra
CASSANDRA_KEYSPACE = "bigdatasube"
# Indice de las columnas leidas en el DataFrame
COL_LATITUD = 0
COL_LONGITUD = 1
COL_ENTIDAD = 5
COL_UBICACION = 7
COL_CUIT = 8
COL_TIPOUBICACION = 9
COL_MODALIDAD = 10
COL_CALLE = 11
COL_NUMERO = 12
COL_BARRIO = 13
COL_COMUNA = 14
COL_PARTIDO = 15
COL_PROVINCIA = 17
COL_LOCALIDAD = 18
COL_CODIGOPOSTAL = 19
