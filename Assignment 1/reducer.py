#!/usr/bin/env python
"""reducer.py"""
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator = ','):
    for line in file:
        yield line.strip().split(separator, 1)

def main():
    # input comes from STDIN (standard input)
    lines = read_mapper_output(file=sys.stdin)
    for current_word, group in groupby(lines, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print "%s%s%d" % (current_word, ",", total_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()



