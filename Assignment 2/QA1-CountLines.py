#Question A.1
import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    spark_session = SparkSession.builder.master("local").appName("Example").getOrCreate()
    spark_context = spark_session.sparkContext
    eng_fname = 'europarl-v7.sv-en.en'
    swed_fname = 'europarl-v7.sv-en.sv'
    try:
        english_lines_rdd = spark_context.textFile(eng_fname).cache()
        logger.info("__LDSA__: Successfully loaded the data from: " + eng_fname)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + eng_fname)
        sys.exit(0)
    try:
        swedish_lines_rdd = spark_context.textFile(swed_fname).cache()
        logger.info("__LDSA__: Successfully loaded the data from: " + swed_fname)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + swed_fname)
        sys.exit(0)

    english_lines_count = english_lines_rdd.count()
    swedish_lines_count = swedish_lines_rdd.count()
    # Reported lines count are = 1862234
    print ("The count of lines in the English transcript: ", english_lines_count)
    print ("The count of lines in the Swedish transcript: ", swedish_lines_count)
    print("The number of partitions:", swedish_lines_count.getNumPartitions())

    # release the cores for another application!
    spark_context.stop()

if __name__ == "__main__":
    main()
