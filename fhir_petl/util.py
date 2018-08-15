import petl as etl
import os
import sys
from uuid import uuid4
from datetime import datetime
from enum import Enum

def dateparser(input_format, iso_format=None):
    def parse(text):
        if text:
            dt = datetime.strptime(text, input_format)
            return FormattedDateTime(dt, iso_format)
        else:
            return None

    return parse

class FormattedDateTime:
    def __init__(self, dt, iso_format):
        if type(iso_format) is not ISOFormat:
            raise TypeError('argument 2 must be an ISOFormat')

        self.dt = dt
        self.format = iso_format

    def __str__(self):
        return self.isoformat()

    def __add__(self, other):
        return FormattedDateTime(self.dt + other, self.format)

    def __sub__(self, other):
        return FormattedDateTime(self.dt - other, self.format)

    def isoformat(self):
        return self.dt.strftime(self.format.value)

class ISOFormat(Enum):
    YEAR = '%Y'
    MONTH = '%Y-%m'
    DAY = '%Y-%m-%d'
    MINUTE = '%Y-%m-%dT%H:%M'
    SECOND = '%Y-%m-%dT%H:%M:%S'

year = dateparser('%Y', ISOFormat.YEAR)

def join(*args):
    result = ''
    for arg in args:
        if arg:
            result += ' %s'%arg
    return result.strip()

number = etl.numparser()

def mkdirp(path):
    if not os.path.exists(path):
        os.makedirs(path)

def resolve(path):
    root = sys.argv[1] if len(sys.argv) > 1 else '.'
    return '{0}/{1}'.format(root, path)

def preprocess(table, source, sort=None, ids=['ID'], convert=int):
    for id in ids:
        table = table.addfield(id, lambda rec: uuid4())
    if sort:
        if convert:
            table = table.convert(sort, convert)
        table = table.sort(sort, buffersize=1000000)
    table.tocsv(source)
