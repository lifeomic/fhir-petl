from datetime import timedelta
import petl as etl
from fhir import to_json, resolve, mkdirp, number, join, year

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
                'subject_id': 'STUDYID',
                'race': ('RACE', map_race),
                'gender': ('SEX', {'F': 'female', 'M': 'male'}),
                'birth_date': ('BIRTH_YR', year),
                'index_date': ('INDEX_YEAR', year)
            }, True)
            .head(100))

index = (patients
         .cut('subject_id', 'id', 'index_date')
         .rename('id', 'subject'))

procedures = (etl.io.csv.fromcsv(resolve('work/procedures.csv'))
              .hashjoin(index, lkey='STUDYID', rkey='subject_id')
              .fieldmap({
                  'id': 'ID',
                  'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                  'code': lambda rec: ('http://www.ama-assn.org/go/cpt', rec['PROC_CODE'], rec['NAME'].strip('" ')),
                  'subject': 'subject'
              }, True)
              .head(1000))

encounters = (etl.io.csv.fromcsv(resolve('work/encounters.csv'))
              .hashjoin(index, lkey='STUDYID', rkey='subject_id'))

conditions = (encounters
              .select('DX_CODE', lambda x: x)
              .fieldmap({
                  'id': 'CONDITION_ID',
                  'onset': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_ADM_INDEX'])),
                  'code': lambda rec: ('http://hl7.org/fhir/sid/icd-9-cm', rec['DX_CODE']),
                  'subject': 'subject'
              }, True)
              .head(1000))

observations = (etl.io.csv.fromcsv(resolve('work/observations.csv'))
                .hashjoin(index, lkey='STUDYID', rkey='subject_id')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                    'code': lambda rec: (None, rec['NAME'], rec['NAME']),
                    'value': lambda rec: number(rec['RESULT_VALUE']) if rec['RESULT_VALUE'] else (rec['CODED_NAME'] or None),
                    'subject': 'subject'
                }, True)
                .select('value', lambda x: x)
                .head(1000))

def medications(rec):
    return [
        ('http://hl7.org/fhir/sid/ndc', rec['NDC_CODE'], rec['DRUG_NAME']),
        ('urn:oid:2.16.840.1.113883.6.68', rec['GPI_CODE'], rec['DRUG_NAME'])
    ]

med_dispenses = (etl.io.csv.fromcsv(resolve('work/med_dispenses.csv'))
                 .hashjoin(index, lkey='CASE_ID', rkey='subject_id')
                 .fieldmap({
                     'id': 'ID',
                     'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                     'medication': medications,
                     'quantity': ('DISPENSE_AMOUNT', number),
                     'daysSupply': ('NUMBER_OF_DAYS_SUPPLY', number),
                     'text': lambda rec: join(rec['DOSAGE_FORM_TEXT'], rec['DRUG_GROUP'], rec['DRUG_CLASS']),
                     'subject': 'subject'
                 }, True)
                 .head(1000))

def medications2(rec):
    return [
        ('http://hl7.org/fhir/sid/ndc', rec['NDC'], rec['ORDER_NAME']),
        ('urn:oid:2.16.840.1.113883.6.68', rec['GPI'], rec['ORDER_NAME'])
    ]

med_requests = (etl.io.csv.fromcsv(resolve('work/med_requests.csv'))
                .hashjoin(index, lkey='STUDYID', rkey='subject_id')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_ORDER_INDEX'])),
                    'medication': medications2,
                    'text': lambda rec: join(rec['DRUG_GROUP'], rec['DRUG_CLASS']),
                    'subject': 'subject'
                }, True)
                .head(1000))


mkdirp(resolve('fhir'))
to_json(patients, 'Patient', resolve('fhir/patients.json'))
to_json(procedures, 'Procedure', resolve('fhir/procedures.json'))
to_json(conditions, 'Condition', resolve('fhir/conditions.json'))
to_json(observations, 'Observation', resolve('fhir/observations.json'))
to_json(med_dispenses, 'MedicationDispense', resolve('fhir/med_dispenses.json'))
to_json(med_requests, 'MedicationRequest', resolve('fhir/med_requests.json'))
