#Question A.2
import re
import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

chars_to_remove = ".,!?#$^&*()"
reg_ex = '[' + re.escape(''.join(chars_to_remove)) + ']'

def pre_process(line):
    new_line = re.sub(reg_ex, ' ', line)
    new_line = new_line.lower().strip()
    if "<" in new_line:
        new_line = ""
    return new_line.split()


def main():
    fname = 'europarl-v7.sv-en.en'

    spark_session = SparkSession.builder.master("spark://192.168.1.20:7077").appName("Application").getOrCreate()
    spark_context = spark_session.sparkContext

    # Import full dataset of newsgroup posts as text file
    try:
        english_lines_rdd = spark_context.textFile(fname).cache()
        logger.info("__LDSA__: Successfully loaded the data from: " + fname)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + fname)
        sys.exit(0)

    english_lines_rdd = english_lines_rdd.map(lambda line: pre_process(line))

    english_lines_count = english_lines_rdd.count()
    # Reported lines count are = 1862234
    print ("The count of lines in the English transcript: ", english_lines_count)
    # release the cores for another application!
    spark_context.stop()


if __name__ == "__main__":
    main()

