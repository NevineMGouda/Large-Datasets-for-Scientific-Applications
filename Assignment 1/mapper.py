
# !/usr/bin/env python
"""mapper.py"""
import sys
import json

LOOKUP_LIST = ["den", "denna", "denne", "det", "han", "hen", "hon"]


def read_input(file):
    for line in file:
        # split the line into words
        yield line.strip()


def main():
    # input comes from STDIN (standard input)
    lines = read_input(sys.stdin)
    for line in lines:
        # remove leading and trailing whitespace
        line = line.strip()
        # skip empty lines
        if len(line) == 0:
            continue
        # load json to a python dictionary
        tweet_dic = json.loads(line)
        if "retweeted_status" in tweet_dic:
            continue
        tweet_text = tweet_dic["text"].strip().lower()
        # tokenize the tweet text into a list of words by splitting with whitespace
        tweet_token = tweet_text.split()
        # This will map the total number of unique tweets available
        print("TotalCount,1")
        # This will output each word with 1 after it to prepare the records for the shuffle&sort and reduce stages.
        for word in tweet_token:
            if word in LOOKUP_LIST:
                print(word + ",1")


if __name__ == "__main__":
    main()




