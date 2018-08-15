from datetime import timedelta
import petl as etl
from fhir_petl.fhir import to_json
from fhir_petl.util import resolve, mkdirp, number, year, dateparser, ISOFormat

date = dateparser('%Y-%m-%d %H:%M:%S', ISOFormat.DAY)

def map_race(race):
    return {
        'AMERICAN INDIAN AND ALASKA NATIVE': ('http://hl7.org/fhir/v3/Race', '1002-5', 'American Indian or Alaska Native'),
        'ASIAN': ('http://hl7.org/fhir/v3/Race', '2028-9', 'Asian'),
        'BLACK OR AFRICAN AMERICAN': ('http://hl7.org/fhir/v3/Race', '2054-5', 'Black or African American'),
        'HISPANIC OR LATINO': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'WHITE': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'MULTIRACIAL': None
    }.get(race, None)

def sample_date(text):
    parser = dateparser('%m/%d/%y %H:%M', ISOFormat.DAY)
    return parser(text)

def birth_date(rec):
    sample_age = timedelta(int(rec['Age (Sample)'])*365.25)
    return sample_date(rec['SAMPLE_DATE']) - sample_age

patients = (etl.io.csv.fromcsv(resolve('work/Patient.csv'))
            .fieldmap({
                'id': 'ID',
                'subject_id': '\uFEFFSID',
                'SID': '\uFEFFSID',
                'race': ('RACE', map_race),
                'gender': ('GENDER', {'F': 'female', 'M': 'male'}),
                'birth_date': birth_date,
                'death_date': ('DEATH_YR', year),
                'sample_date': ('SAMPLE_DATE', sample_date)
            }, True))

index = (patients
         .cut('SID', 'id', 'sample_date')
         .rename('id', 'subject'))

def proc_code(rec):
    if rec['PROC_SYS_ID'] == 1:
        return ('http://www.ama-assn.org/go/cpt', rec['PROC_CODE'], rec['PROC_NAME'].strip('" '))
    else:
        return ('http://hl7.org/fhir/sid/icd-9-cm', rec['PROC_CODE'], rec['PROC_NAME'].strip('" '))

procedures = (etl.io.csv.fromcsv(resolve('work/Procedure.csv'))
              .hashjoin(index, lkey='SID', rkey='SID')
              .fieldmap({
                  'id': 'ID',
                  'date': lambda rec: date(rec['PROC_DATE'] or rec['ARRIVE_DATE'] or rec['DISCHARGE_DATE']),
                  'code': proc_code,
                  'subject': 'subject'
              }, True)
              .head(1000))

def cv_code(rec):
    codes = []
    if rec['LOINC_CODE']:
        codes.append(('http://loinc.org', rec['LOINC_CODE'], rec['CV_NAME']))
    if rec['CV_CODE']:
        codes.append((None, rec['CV_CODE'], rec['CV_NAME']))
    return codes

def cv_value(rec):
    if rec['CV_RESULT_NUMERIC']:
        return number(rec['CV_RESULT_NUMERIC'])
    else:
        return rec['CV_RESULT_CATEGORICAL'] or None

observations = (etl.io.csv.fromcsv(resolve('work/Observation.csv'))
                .hashjoin(index, lkey='SID', rkey='SID')
                .fieldmap({
                    'id': 'ID',
                    'date': lambda rec: rec['sample_date'] + timedelta(int(rec['Date_VIS_Sample'])),
                    'code': cv_code,
                    'value': cv_value,
                    'subject': 'subject'
                }, True)
                .select('value', lambda x: x)
                .head(1000))

mkdirp(resolve('fhir'))
to_json(patients, 'Patient', resolve('fhir/Patient.json'))
to_json(procedures, 'Procedure', resolve('fhir/Procedure.json'))
to_json(observations, 'Observation', resolve('fhir/Observation.json'))
