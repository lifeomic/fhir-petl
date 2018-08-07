import json
import petl as etl
from uuid import uuid4
import os
import sys

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

def to_json(table, resourceType, source):
    mapper = types[resourceType]
    table = table.fieldmap({'data': mapper}, True)
    etl.io.text.totext(table, source, 'utf8', template='{data}\n')
    return table

def to_codeable_concept(x):
    if type(x) is list:
        result = map(tuple_to_code, x)
    else:
        result = [tuple_to_code(x)]
    return {'coding': [c for c in result if c.get('code')]}

def tuple_to_code(x):
    if len(x) == 2:
        display = None
        system, code = x
    else:
        system, code, display = x

    if system and code and display:
        return {'system': system, 'code': code, 'display': display}
    elif code and display:
        return {'code': code, 'display': display}
    elif system and code:
        return {'system': system, 'code': code}
    elif system and display:
        return {'system': system, 'display': display}
    elif code:
        return {'code': code}
    elif display:
        return {'display': display}
    elif system:
        return {'system': system}

def to_patient(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Patient'
    if rec['subject_id']:
        result['identifier'] = [
            {
                'type': to_codeable_concept(('http://hl7.org/fhir/v2/0203', 'ANON')),
                'system': 'http://lifeomic.com/fhir/subject-id',
                'value': rec['subject_id']
            }
        ]
    if rec['race']:
        result['extension'] = [
            {
                'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
                'valueCodeableConcept': to_codeable_concept(rec['race'])
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
        result['code'] = to_codeable_concept(rec['code'])
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
        result['code'] = to_codeable_concept(rec['code'])
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
        result['code'] = to_codeable_concept(rec['code'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if rec['value']:
        value = rec['value']
        if type(value) is int or type(value) is float:
            result['valueQuantity'] = {'value': value}
        else:
            result['valueString'] = value
    return json.dumps(result)

def to_med_dispense(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'MedicationDispense'
    if rec['date']:
        result['whenHandedOver'] = rec['date'].isoformat()
    if rec['medication']:
        result['medicationCodeableConcept'] = to_codeable_concept(rec['medication'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if rec['quantity']:
        result['quantity'] = {'value': rec['quantity']}
    if rec['daysSupply']:
        result['daysSupply'] = {'value': rec['quantity'], 'unit': 'days'}
    if rec['text']:
        result['text'] = {'div': rec['text']}
    return json.dumps(result)

def to_med_request(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'MedicationRequest'
    if rec['date']:
        result['date'] = rec['date'].isoformat()
    if rec['medication']:
        result['medicationCodeableConcept'] = to_codeable_concept(rec['medication'])
    if rec['subject']:
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if rec['text']:
        result['text'] = {'div': rec['text']}
    return json.dumps(result)

types = {
    'Procedure': to_procedure,
    'Patient': to_patient,
    'Condition': to_condition,
    'Observation': to_observation,
    'MedicationDispense': to_med_dispense,
    'MedicationRequest': to_med_request
}
