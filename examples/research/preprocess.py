import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp

mkdirp(resolve('work'))

preprocess(
    etl.io.csv.fromcsv(resolve('case_demog_27065.csv')),
    resolve('work/patients.csv'),
    'STUDYID')

preprocess(
    etl.io.csv.fromtsv(resolve('dx_case_inst.txt')),
    resolve('work/encounters.csv'),
    ('STUDYID', 'INST_FAKE', 'DAYS_ADM_INDEX', 'DAYS_DISCH_INDEX'),
    ('ID', 'CONDITION_ID'))

preprocess(
    etl.io.csv.fromtsv(resolve('lab_case_inst.txt')),
    resolve('work/observations.csv'),
    'STUDYID')

preprocess(
    etl.io.csv.fromtsv(resolve('med_case_inst_gpi.txt')),
    resolve('work/med_dispenses.csv'),
    'CASE_ID')

preprocess(
    etl.io.csv.fromtsv(resolve('order_case_inst_gpi.txt')),
    resolve('work/med_requests.csv'),
    'STUDYID')

preprocess(
    etl.io.csv.fromtsv(resolve('proc_case_inst.txt')),
    resolve('work/procedures.csv'),
    'STUDYID')
