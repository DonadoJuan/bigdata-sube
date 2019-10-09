import os
from src.clases import Constantes
from src.clases.SparkHelper import SparkHelper


def main():

    print("Iniciando...")
    sh = SparkHelper()

    # Leemos el archivo CSV del origen
    print('Leyendo el archivo ' + Constantes.SOURCE_FILE + ' mediante Spark...')
    dataframe = sh.readcsv(Constantes.SOURCE_FILE, True)

    # Escribimos el dataframe en los distintos formatos en el HDFS
    sh.writetoparquet(dataframe, Constantes.HDFS_OUTPUT_DIR, Constantes.PARQUET_FILE)
    print('Se escribió el archivo "resultado.parquet" en formato Parquet en "' + Constantes.HDFS_OUTPUT_DIR + '"')
    print('===== Fin etapa 1 =====')
    input('Presione ENTER para continuar...')

    # Leemos el archivo en formato parquet del HDFS
    print('Leyendo el archivo en formato Parquet del HDFS')
    rutaarchivoParquet = os.path.join(Constantes.HDFS_OUTPUT_DIR, Constantes.PARQUET_FILE)
    dataframeParquet = sh.readparquet(rutaarchivoParquet)

    # Escribimos en Cassandra
    print('Escribiendo el DataFrame en Cassandra...')
    cantidad = sh.writetocassandra(dataframeParquet)
    print('Se escribieron ' + str(cantidad) + ' registros en BigDataSube.PuntosDeCarga')
    input('Presione ENTER para continuar...')

    return 0


if __name__ == "__main__":
    main()

