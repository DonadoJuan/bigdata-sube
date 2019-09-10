from src.clases.SparkHelper import SparkHelper


def main():

    SOURCE_FILE = "data/data-source.csv"
    HDFS_OUTPUT = "/Grupo1/procesado/result.txt"

    sh = SparkHelper()
    dataframe = sh.readcsv(SOURCE_FILE, True)
    dataframe.show()
    return 0


if __name__ == "__main__":
    main()
