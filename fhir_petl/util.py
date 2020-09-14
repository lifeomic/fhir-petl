from datetime import datetime
from enum import Enum
from uuid import uuid4
import os
import sys
import petl as etl

# Parse a string using input_format into
# a datetime and use the output_format to
# render the datetime as a string
def dateparser(input_format, output_format):
    def parse(text):
        if text:
            dt = datetime.strptime(text, input_format)
            return FormattedDateTime(dt, output_format)

        return None

    return parse


class FormattedDateTime:
    def __init__(self, dt, output_format):
        if not isinstance(output_format, ISOFormat):
            raise TypeError("argument 2 must be an ISOFormat")

        self.dt = dt
        self.format = output_format

    def __str__(self):
        return self.isoformat()

    def __add__(self, other):
        return FormattedDateTime(self.dt + other, self.format)

    def __sub__(self, other):
        return FormattedDateTime(self.dt - other, self.format)

    def isoformat(self):
        return self.dt.strftime(self.format.value)


class ISOFormat(Enum):
    YEAR = "%Y"
    MONTH = "%Y-%m"
    DAY = "%Y-%m-%d"
    MINUTE = "%Y-%m-%dT%H:%M"
    SECOND = "%Y-%m-%dT%H:%M:%S"


# parse a year string
year = dateparser("%Y", ISOFormat.YEAR)

# join one are more inputs into a string
# separated by space. falsy arguments are
# ignored
def join(*args):
    result = ""
    for arg in args:
        if arg:
            result += " %s" % arg
    return result.strip()


number = etl.numparser()

# recursively make directories
def mkdirp(path):
    if not os.path.exists(path):
        os.makedirs(path)


# resolve a relative path into an absolute
# path using the command line args
def resolve(path):
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    return "{0}/{1}".format(root, path)


# preprocess a table to get it ready for ETL
def preprocess(table, sort=None, ids=None, convert=int):
    if not ids:
        ids = ["ID"]

    for id in ids:
        table = table.addfield(id, lambda rec: uuid4())
    if sort:
        if convert:
            table = table.convert(sort, convert)
        table = table.sort(sort, buffersize=1000000)

    return table
