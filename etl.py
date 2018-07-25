from collections import OrderedDict
from datetime import timedelta
import fhir
from fhir import to_json, to_patient, to_procedure, to_condition, to_observation, resolve, mkdirp, number
import petl as etl

parse_year = etl.dateparser('%Y')

def map_race(race):
    return {
        'AMERICAN INDIAN AND ALASKA NATIVE': ('1002-5', 'American Indian or Alaska Native'),
        'ASIAN': ('2028-9', 'Asian'),
        'BLACK OR AFRICAN AMERICAN': ('2054-5', 'Black or African American'),
        'HISPANIC OR LATINO': ('2106-3', 'White'),
        'WHITE': ('2106-3', 'White'),
        'MULTIRACIAL': None
    }.get(race, None)

patients = (etl.io.csv.fromcsv(resolve('work/patients.csv'))
            .fieldmap({
                'id': 'ID',
                'subject_id': 'STUDYID',
                'race': ('RACE', map_race),
                'gender': ('SEX', {'F': 'female', 'M': 'male'}),
                'birth_date': ('BIRTH_YR', parse_year),
                'index_date': ('INDEX_YEAR', parse_year)
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
                  'code': lambda rec: ('http://hl7.org/fhir/sid/icd-9-cm', rec['DX_CODE'], None),
                  'subject': 'subject'
              }, True)
              .head(1000))

observations = (etl.io.csv.fromcsv(resolve('work/observations.csv'))
                .hashjoin(index, lkey='STUDYID', rkey='subject_id')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['index_date'] + timedelta(int(rec['DAYS_VIS_INDEX'])),
                    'code': lambda rec: (None, rec['NAME']),
                    'value': lambda rec: number(rec['RESULT_VALUE']) if rec['RESULT_VALUE'] else (rec['CODED_NAME'] or None),
                    'subject': 'subject'
                }, True)
                .select('value', lambda x: x)
                .head(1000))

mkdirp(resolve('fhir'))
to_json(patients, to_patient, resolve('fhir/patients.json'))
to_json(procedures, to_procedure, resolve('fhir/procedures.json'))
to_json(conditions, to_condition, resolve('fhir/conditions.json'))
to_json(observations, to_observation, resolve('fhir/observations.json'))