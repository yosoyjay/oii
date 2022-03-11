import csv
import re

NO_LIMIT=-1

"""Utilities for reading and writing CSV. uses source/sink from io"""

def parse_csv_row(row, schema=None):
    if schema is None:
        return dict(list(zip(list(range(len(row))), row)))
    else:
        #I moved this here, since it errors above if scheme is None; can't length None
        row = row[:len(schema)] # ignore trailing fields
        return dict([(colname,cast(value)) for (colname,cast),value in zip(schema,row)])

def read_csv(source, schema=None, offset=0, limit=NO_LIMIT):
    with source as csvdata:
        for row in csv.reader(csvdata):
            if offset <= 0:
                if limit == 0:
                    return
                limit -= 1
                yield parse_csv_row(row,schema)
            else:
                offset -= 1

def csv_quote(thing):
    """For a given string that is to appear in CSV output, quote it if it is non-numeric"""
    if re.match(r'^-?[0-9]*(\.[0-9]+)?$',thing):
        return thing
    else:
        return '"' + thing + '"'

def csv_str(v,numeric_format='%.12f'):
    """For a given value, produce a CSV representation of it"""
    try:
        return re.sub(r'\.$','',(numeric_format % v).rstrip('0'))
    except:
        return str(v)
            
