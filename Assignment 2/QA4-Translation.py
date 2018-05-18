# Question A.4
import os
import re
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

os.environ["SPARK_HOME"]="/usr/local/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
chars_to_remove = ".,!?#$"
reg_ex = '[' + re.escape(''.join(chars_to_remove)) + ']'

def pre_process(line):
    new_line = re.sub(reg_ex, ' ', line)
    new_line = new_line.lower().strip()
    if "<" in new_line:
        new_line = ""
    return new_line.split()

def main():
    conf = SparkConf().setAppName("Translation").setMaster("spark://192.168.1.20:7077")
    spark_context = SparkContext(conf=conf)
    # spark_session = SparkSession.builder.master("local").appName("Example").getOrCreate()
    # spark_context = spark_session.sparkContext

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

    english_lines_rdd = english_lines_rdd.zipWithIndex().map(lambda pair: (pair[1],pair[0]))
    swedish_lines_rdd = swedish_lines_rdd.zipWithIndex().map(lambda pair: (pair[1],pair[0]))

    paired_lines_rdd = english_lines_rdd.join(swedish_lines_rdd).\
                        sortBy(lambda pair: pair[1]).\
                        map(lambda pair: (pair[0],(pre_process(pair[1][0]), pre_process(pair[1][1])))). \
                        filter(lambda pair: (len(pair[1][0]) > 0 and len(pair[1][1]) > 0) and
                                                    (len(pair[1][0]) == len(pair[1][1])) and
                                                    (len(pair[1][0]) <= 30 and len(pair[1][1]) <= 30)).\
                        flatMap(lambda pair: zip(pair[1][0], pair[1][1])).\
                        map(lambda pair: (pair,1)).reduceByKey(lambda a, b: a + b).\
                        sortBy(lambda pair: pair[1], ascending=False)
    for word,count in paired_lines_rdd.take(100):
        english_word = word[0]
        swedish_word = word[1]
        print ("Translation of \"", english_word, "\" is", swedish_word, ". With occurences with count =", count)
    spark_context.stop()


if __name__ == "__main__":
    main()
