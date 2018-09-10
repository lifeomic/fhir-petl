import json
import petl as etl

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

def has(rec, field):
    return field in rec.flds and rec[field]

def to_patient(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Patient'
    if has(rec, 'subject_id'):
        result['identifier'] = [
            {
                'type': to_codeable_concept(('http://hl7.org/fhir/v2/0203', 'ANON')),
                'system': 'http://lifeomic.com/fhir/subject-id',
                'value': rec['subject_id']
            }
        ]
    if has(rec, 'race'):
        result['extension'] = [
            {
                'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
                'valueCodeableConcept': to_codeable_concept(rec['race'])
            }
        ]
    if has(rec, 'gender'):
        result['gender'] = rec['gender']
    if has(rec, 'birth_date'):
        result['birthDate'] = rec['birth_date'].isoformat()
    if has(rec, 'death_date'):
        result['deceasedDateTime'] = rec['death_date'].isoformat()
    if has(rec, 'tag'):
        result['meta'] = {'tag': [tuple_to_code(rec['tag'])]}
    return json.dumps(result)

def to_procedure(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Procedure'
    if has(rec, 'date'):
        result['performedDateTime'] = rec['date'].isoformat()
    if has(rec, 'code'):
        result['code'] = to_codeable_concept(rec['code'])
    if has(rec, 'subject'):
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if has(rec, 'note'):
        result['note'] = [{'text': rec['note']}]
    return json.dumps(result)

def to_condition(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Condition'
    if has(rec, 'onset'):
        result['onsetDateTime'] = rec['onset'].isoformat()
    if has(rec, 'asserted'):
        result['assertedDate'] = rec['asserted'].isoformat()
    if has(rec, 'code'):
        result['code'] = to_codeable_concept(rec['code'])
    if has(rec, 'subject'):
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if has(rec, 'note'):
        result['note'] = [{'text': rec['note']}]
    return json.dumps(result)

def to_observation(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'Observation'
    if has(rec, 'date'):
        result['effectiveDateTime'] = rec['date'].isoformat()
    if has(rec, 'code'):
        result['code'] = to_codeable_concept(rec['code'])
    if has(rec, 'subject'):
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if has(rec, 'value'):
        value = rec['value']
        if type(value) is int or type(value) is float:
            result['valueQuantity'] = {'value': value}
        else:
            result['valueString'] = value
    if has(rec, 'note'):
        result['note'] = [{'text': rec['note']}]
    return json.dumps(result)

def to_med_dispense(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'MedicationDispense'
    if has(rec, 'date'):
        result['whenHandedOver'] = rec['date'].isoformat()
    if has(rec, 'medication'):
        result['medicationCodeableConcept'] = to_codeable_concept(rec['medication'])
    if has(rec, 'subject'):
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if has(rec, 'quantity'):
        result['quantity'] = {'value': rec['quantity']}
    if has(rec, 'daysSupply'):
        result['daysSupply'] = {'value': rec['quantity'], 'unit': 'days'}
    if has(rec, 'note'):
        result['note'] = [{'text': rec['note']}]
    return json.dumps(result)

def to_med_request(rec):
    result = {}
    result['id'] = rec['id']
    result['resourceType'] = 'MedicationRequest'
    if has(rec, 'date'):
        result['authoredOn'] = rec['date'].isoformat()
    if has(rec, 'medication'):
        result['medicationCodeableConcept'] = to_codeable_concept(rec['medication'])
    if has(rec, 'subject'):
        result['subject'] = {'reference': 'Patient/' + rec['subject']}
    if has(rec, 'note'):
        result['note'] = [{'text': rec['note']}]
    if has(rec, 'status'):
        result['status'] = rec['status']
    return json.dumps(result)

types = {
    'Procedure': to_procedure,
    'Patient': to_patient,
    'Condition': to_condition,
    'Observation': to_observation,
    'MedicationDispense': to_med_dispense,
    'MedicationRequest': to_med_request
}
