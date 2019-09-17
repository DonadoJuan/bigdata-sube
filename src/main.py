from src.clases.SparkHelper import SparkHelper


def main():

    SOURCE_FILE = "data/data-source.csv"
    HDFS_OUTPUT_DIR = "hdfs://kudu:20500/Grupo1/procesado/"

    print("Iniciando...")
    sh = SparkHelper()

    # Leemos el archivo CSV del origen
    print("Leyendo el archivo " + SOURCE_FILE + " mediante Spark...")
    dataframe = sh.readcsv(SOURCE_FILE, True)

    # Escribimos el dataframe en los distintos formatos en el HDFS
    sh.writetoparquet(dataframe, HDFS_OUTPUT_DIR, "resultado.parquet")
    print('Se escribió el archivo "resultado.parquet" en formato Parquet en "' + HDFS_OUTPUT_DIR + '"')

    return 0


if __name__ == "__main__":
    main()
