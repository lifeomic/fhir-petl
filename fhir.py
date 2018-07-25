import json
import petl as etl
from uuid import uuid4
import os
import sys

def number(x):
    try:
        return int(x)
    except ValueError:
        return float(x)

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

def to_json(table, mapper, source):
    table = table.fieldmap({'data': mapper}, True)
    etl.io.text.totext(table, source, 'utf8', template='{data}\n')
    return table

def tuple_to_codeable_concept(x, defaultSystem=None):
    if len(x) == 2:
        system = defaultSystem
        code, display = x
    else:
        system, code, display = x

    if system and code and display:
        return {'coding': [{'system': system, 'code': code, 'display': display}]}
    elif code and display:
        return {'coding': [{'code': code, 'display': display}]}
    elif system and code:
        return {'coding': [{'system': system, 'code': code}]}
    elif system and display:
        return {'coding': [{'system': system, 'display': display}]}
    elif code:
        return {'coding': [{'code': code}]}
    elif display:
        return {'coding': [{'display': display}]}
    elif system:
        return {'coding': [{'system': system}]}

def to_patient(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Patient'
    if rec['subject_id']:
        result['identifier'] = [
            {
                'type': tuple_to_codeable_concept(('http://hl7.org/fhir/v2/0203', 'ANON')),
                'system': 'http://lifeomic.com/fhir/subject-id',
                'value': rec['subject_id']
            }
        ]
    if rec['race']:
        result['extension'] = [
            {
                'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
                'valueCodeableConcept': tuple_to_codeable_concept(rec['race'], 'http://hl7.org/fhir/v3/Race')
            }
        ]
    if rec['gender']:
        result['gender'] = rec['gender']
    if rec['birth_date']:
        result['birthDate'] = rec['birth_date'].isoformat()
    return json.dumps(result)

def to_procedure(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Procedure'
    if rec['date']:
        result['performedDateTime'] = rec['date'].isoformat()
    if rec['code']:
        result['code'] = tuple_to_codeable_concept(rec['code'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    return json.dumps(result)

def to_condition(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Condition'
    if rec['onset']:
        result['onsetDateTime'] = rec['onset'].isoformat()
    if rec['code']:
        result['code'] = tuple_to_codeable_concept(rec['code'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    return json.dumps(result)

def to_observation(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Observation'
    if rec['date']:
        result['effectiveDateTime'] = rec['date'].isoformat()
    if rec['code']:
        result['code'] = tuple_to_codeable_concept(rec['code'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if rec['value']:
        value = rec['value']
        if type(value) is int or type(value) is float:
            result['valueQuantity'] = {'value': value}
        else:
            result['valueString'] = value
    return json.dumps(result)