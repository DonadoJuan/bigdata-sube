import os

from src.clases.SparkHelper import SparkHelper


def main():

    SOURCE_FILE = "data/data-source.csv"
    HDFS_OUTPUT_DIR = "hdfs://kudu:20500/Grupo1/procesado/"
    PARQUET_FILE = "resultado.parquet"

    print("Iniciando...")
    sh = SparkHelper()

    # Leemos el archivo CSV del origen
    print('Leyendo el archivo ' + SOURCE_FILE + ' mediante Spark...')
    dataframe = sh.readcsv(SOURCE_FILE, True)

    # Escribimos el dataframe en los distintos formatos en el HDFS
    sh.writetoparquet(dataframe, HDFS_OUTPUT_DIR, PARQUET_FILE)
    print('Se escribió el archivo "resultado.parquet" en formato Parquet en "' + HDFS_OUTPUT_DIR + '"')
    print('===== Fin etapa 1 =====')
    input('Presione ENTER para continuar...')

    # Leemos el archivo en formato parquet del HDFS
    print('Leyendo el archivo en formato Parquet del HDFS')
    rutaarchivoParquet = os.path.join(HDFS_OUTPUT_DIR, PARQUET_FILE)
    dataframeParquet = sh.readparquet(rutaarchivoParquet)

    # Escribimos en Cassandra
    print('Escribiendo el DataFrame en Cassandra...')
    sh.writetocassandra(dataframeParquet)

    return 0


if __name__ == "__main__":
    main()
