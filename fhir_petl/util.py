import petl as etl
import os
import sys
from uuid import uuid4

def join(*args):
    result = ''
    for arg in args:
        if arg:
            result += ' %s'%arg
    return result.strip()

number = etl.numparser()
year = etl.dateparser('%Y')

def mkdirp(path):
    path = resolve(path)
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
