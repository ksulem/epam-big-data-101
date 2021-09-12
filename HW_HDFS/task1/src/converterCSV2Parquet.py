from pyspark.sql import SparkSession, SQLContext
import argparse
import traceback

spark = SparkSession\
      .builder\
      .appName("PythonFileConverter")\
      .getOrCreate()


def convertCSVtoParquet(source_path, target_path):
    """
    Load file from source path
    Convert file from CSV to Parquet
    Save file to target path
    """
    try:
        df = spark.read.csv(source_path,
                            header="true")
    except Exception():
        trace_back = traceback.format_exc()
        return 1

    try:
        df.repartition(1).write.mode('overwrite').parquet(target_path)
    except Exception:
        trace_back = traceback.format_exc()
        return 1

    return 0


def getArgs():
    """
    function to get arguments from Command Line
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('-sp', '--sourceFilePath', type=str, required=True)
    argument_parser.add_argument('-tp', '--targetFilePath', type=str, required=True)
    parsed_args = argument_parser.parse_args()

    src_filepath = parsed_args.sourceFilePath
    trg_filepath = parsed_args.targetFilePath

    return {'source_path': src_filepath,
            'target_path': trg_filepath}


def executeConversion(**parsed_args):
    """
    executor of conversion
    :param parsed_args:
    :return:
    """
    conversion = None
    conversion = convertCSVtoParquet(parsed_args['source_path'],
                                     parsed_args['target_path'])
    return conversion


if __name__ == '__main__':
    parsed_args = getArgs()
    executeConversion(**parsed_args)
