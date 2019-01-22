from datetime import timedelta
import petl as etl
from fhir_petl.fhir import to_json
from fhir_petl.util import resolve, mkdirp, number, year, dateparser, ISOFormat

date = dateparser('%Y-%m-%d %H:%M:%S', ISOFormat.DAY)

def map_race(race):
    return {
        'ASIAN': ('http://hl7.org/fhir/v3/Race', '2028-9', 'Asian'),
        'BLACK': ('http://hl7.org/fhir/v3/Race', '2054-5', 'Black or African American'),
        'HISPANIC/LATINO': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'WHITE': ('http://hl7.org/fhir/v3/Race', '2106-3', 'White'),
        'NATIVE HAWAIIAN/PACIFIC ISLANDER': ('http://hl7.org/fhir/v3/Race', '2076-8', 'Native Hawaiian or Other Pacific Islander')
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
                'sample_date': ('SAMPLE_DATE', sample_date),
                'tag': ('Cohort', lambda cohort: ('cohort', cohort.upper()))
            }, True))

index = (patients
         .cut('SID', 'id', 'sample_date')
         .rename('id', 'subject'))

def proc_code(rec):
    if rec['PROC_SYS_ID'] == 1:
        return ('http://www.ama-assn.org/go/cpt', rec['PROC_CODE'], rec['PROC_NAME'].strip('" '))

    return ('http://hl7.org/fhir/sid/icd-9-cm', rec['PROC_CODE'], rec['PROC_NAME'].strip('" '))

procedures = (etl.io.csv.fromcsv(resolve('work/Procedure.csv'))
              .hashjoin(index, lkey='SID', rkey='SID')
              .fieldmap({
                  'id': 'ID',
                  'date': lambda rec: date(rec['PROC_DATE'] or rec['ARRIVE_DATE'] or rec['DISCHARGE_DATE']),
                  'code': proc_code,
                  'subject': 'subject',
                  'note': 'ENC_TYPE'
              }, True))

def cv_code(rec):
    codes = []
    if rec['LOINC_CODE']:
        codes.append(('http://loinc.org', rec['LOINC_CODE'], rec['CV_NAME']))
    elif rec['CV_CODE']:
        codes.append(('http://regenstrief.org/cv_code', rec['CV_CODE'], rec['CV_NAME']))
    return codes

def cv_value(rec):
    if rec['CV_RESULT_NUMERIC']:
        return number(rec['CV_RESULT_NUMERIC'])

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
                .selecttrue('value'))

def dx_code(rec):
    if rec['DX_SYS_ID'] == '9':
        return ('http://hl7.org/fhir/sid/icd-9-cm', rec['DX_CODE'], rec['DX_NAME'])

    return ('http://hl7.org/fhir/sid/icd-10', rec['DX_CODE'], rec['DX_NAME'])

conditions = (etl.io.csv.fromcsv(resolve('work/Condition.csv'))
              .hashjoin(index, lkey='SID', rkey='SID')
              .fieldmap({
                  'id': 'ID',
                  'onset': ('DATE_OF_DX', date),
                  'code': dx_code,
                  'subject': 'subject'
              }, True))

med_requests = (etl.io.csv.fromcsv(resolve('work/MedicationRequest.csv'))
                .hashjoin(index, lkey='SID', rkey='SID')
                .fieldmap({
                    'id': 'ID',
                    'date': ('ORDER_DATE', date),
                    'medication': lambda rec: ('http://hl7.org/fhir/sid/ndc', rec['NDC'], rec['DRUG_NAME']),
                    'subject': 'subject',
                    'status': ('FILL_COMPLETE', lambda complete: 'completed' if complete == '1' else None)
                }, True))

mkdirp(resolve('fhir'))
to_json(patients, 'Patient', resolve('fhir/Patient.json'))
to_json(procedures, 'Procedure', resolve('fhir/Procedure.json'))
to_json(observations, 'Observation', resolve('fhir/Observation.json'))
to_json(conditions, 'Condition', resolve('fhir/Condition.json'))
to_json(med_requests, 'MedicationRequest', resolve('fhir/MedicationRequest.json'))
