from datetime import timedelta
import petl as etl
from fhir_petl.fhir import to_json
from fhir_petl.util import resolve, mkdirp, number, join, year, dateparser, ISOFormat

def map_race(race):
    return {
        'AMERICAN INDIAN AND ALASKA NATIVE': ('http://hl7.org/fhir/v3/Race', '1002-5', 'American Indian or Alaska Native'),
        'ASIAN': ('http://hl7.org/fhir/v3/Race', '2028-9', 'Asian'),
        'BLACK OR AFRICAN AMERICAN': ('http://hl7.org/fhir/v3/Race', '2054-5', 'Black or African American'),
        'HISPANIC OR LATINO': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'WHITE': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'MULTIRACIAL': None
    }.get(race, None)

patients = (etl.io.csv.fromcsv(resolve('work/patients.csv'))
            .fieldmap({
                'id': 'ID',
                'STUDYID': 'STUDYID',
                'subject_id': ('STUDYID', lambda x: 'CASE-' + x),
                'race': ('RACE', map_race),
                'gender': ('SEX', {'F': 'female', 'M': 'male'}),
                'birth_date': ('BIRTH_YR', year),
                'index_date': ('INDEX_YEAR', dateparser('%Y', ISOFormat.DAY)),
                'tag': lambda rec: ('subject-type', 'case')
            }, True))

index = (patients
         .cut('STUDYID', 'id', 'index_date')
         .rename('id', 'subject'))

procedures = (etl.io.csv.fromcsv(resolve('work/procedures.csv'))
              .hashjoin(index, lkey='STUDYID', rkey='STUDYID')
              .fieldmap({
                  'id': 'ID',
                  'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                  'code': lambda rec: ('http://www.ama-assn.org/go/cpt', rec['PROC_CODE'], rec['NAME'].strip('" ')),
                  'subject': 'subject'
              }, True))

encounters = (etl.io.csv.fromcsv(resolve('work/encounters.csv'))
              .hashjoin(index, lkey='STUDYID', rkey='STUDYID'))

conditions = (encounters
              .select('DX_CODE', lambda x: x)
              .fieldmap({
                  'id': 'CONDITION_ID',
                  'onset': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_ADM_INDEX'])),
                  'code': lambda rec: ('http://hl7.org/fhir/sid/icd-9-cm', rec['DX_CODE']),
                  'note': lambda rec: join(rec['CARE_SETTING_TEXT'], rec['LOCATION_POINT_OF_CARE']),
                  'subject': 'subject'
              }, True))

observations = (etl.io.csv.fromcsv(resolve('work/observations.csv'))
                .hashjoin(index, lkey='STUDYID', rkey='STUDYID')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                    'code': lambda rec: (None, rec['NAME'], rec['NAME']),
                    'value': lambda rec: number(rec['RESULT_VALUE']) if rec['RESULT_VALUE'] else (rec['CODED_NAME'] or None),
                    'subject': 'subject'
                }, True)
                .select('value', lambda x: x))

def medications(rec):
    group = rec['DRUG_GROUP'].strip('*')
    clazz = rec['DRUG_CLASS'].strip('*')
    return [
        ('http://hl7.org/fhir/sid/ndc', rec['NDC_CODE'], rec['DRUG_NAME']),
        ('urn:oid:2.16.840.1.113883.6.68', rec['GPI_CODE']),
        ('drug-class', clazz, clazz),
        ('drug-group', group, group)
    ]

med_dispenses = (etl.io.csv.fromcsv(resolve('work/med_dispenses.csv'))
                 .hashjoin(index, lkey='CASE_ID', rkey='STUDYID')
                 .fieldmap({
                     'id': 'ID',
                     'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                     'medication': medications,
                     'quantity': ('DISPENSE_AMOUNT', number),
                     'daysSupply': ('NUMBER_OF_DAYS_SUPPLY', number),
                     'subject': 'subject'
                 }, True))

def medications2(rec):
    group = rec['DRUG_GROUP'].strip('*')
    clazz = rec['DRUG_CLASS'].strip('*')
    return [
        ('http://hl7.org/fhir/sid/ndc', rec['NDC'], rec['ORDER_NAME']),
        ('urn:oid:2.16.840.1.113883.6.68', rec['GPI']),
        ('drug-class', clazz, clazz),
        ('drug-group', group, group)
    ]

med_requests = (etl.io.csv.fromcsv(resolve('work/med_requests.csv'))
                .hashjoin(index, lkey='STUDYID', rkey='STUDYID')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_ORDER_INDEX'])),
                    'medication': medications2,
                    'subject': 'subject'
                }, True))


mkdirp(resolve('fhir'))
to_json(patients, 'Patient', resolve('fhir/patients.json'))
to_json(procedures, 'Procedure', resolve('fhir/procedures.json'))
to_json(conditions, 'Condition', resolve('fhir/conditions.json'))
to_json(observations, 'Observation', resolve('fhir/observations.json'))
to_json(med_dispenses, 'MedicationDispense', resolve('fhir/med_dispenses.json'))
to_json(med_requests, 'MedicationRequest', resolve('fhir/med_requests.json'))
