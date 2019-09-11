from src.clases.SparkHelper import SparkHelper


def main():

    SOURCE_FILE = "data/data-source.csv"
    HDFS_OUTPUT_DIR = "hdfs://kudu:20500/Grupo1/procesado/"

    sh = SparkHelper()
    # Leemos el archivo CSV del origen
    dataframe = sh.readcsv(SOURCE_FILE, True)
    # Escribimos el dataframe en formato Parquet en el HDFS
    sh.writetoparquet(dataframe, HDFS_OUTPUT_DIR, "resultado.parquet")
    return 0


if __name__ == "__main__":
    main()
