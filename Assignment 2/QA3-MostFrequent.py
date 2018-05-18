# Question A.3
import re
import sys
import logging
from pyspark.sql import SparkSession

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

    spark_session = SparkSession.builder.master("local").appName("Example").getOrCreate()
    spark_context = spark_session.sparkContext

    eng_fname = 'europarl-v7.sv-en.en'
    swed_fname = 'europarl-v7.sv-en.sv'
    try:
        english_words_rdd = spark_context.textFile(eng_fname).cache()
        logger.info("__LDSA__: Successfully loaded the data from: " + eng_fname)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + eng_fname)
        sys.exit(0)
    try:
        swedish_words_rdd = spark_context.textFile(swed_fname).cache()
        logger.info("__LDSA__: Successfully loaded the data from: " + swed_fname)
    except:
        logger.error("__LDSA__: Something went wrong while importing from: " + swed_fname)
        sys.exit(0)

    english_words_rdd = english_words_rdd.flatMap(lambda line: pre_process(line)).\
                                            map(lambda word: (word, 1)).\
                                            reduceByKey(lambda a, b: a + b).\
                                            sortBy(lambda pair: pair[1], ascending=False)

    swedish_words_rdd = swedish_words_rdd.flatMap(lambda line: pre_process(line)).\
                                            map(lambda word: (word, 1)).\
                                            reduceByKey(lambda a, b: a + b).\
                                            sortBy(lambda pair: pair[1], ascending=False)

    print ("The 10 most frequent English words are (ordered from most to least frequent): ")
    for word, count in english_words_rdd.take(10):
        print (word,)

    print ("\nThe 10 most frequent Swedish words are(ordered from most to least frequent): ")
    for word, count in swedish_words_rdd.take(10):
        print (word,)

    # release the cores for another application!
    spark_context.stop()


if __name__ == "__main__":
    main()
