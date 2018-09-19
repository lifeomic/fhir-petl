import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp

mkdirp(resolve('work'))

preprocess(
    etl.io.csv.fromcsv(resolve('case_demog_27065.csv')),
    'STUDYID'
).tocsv(resolve('work/patients.csv'))

preprocess(
    etl.io.csv.fromtsv(resolve('dx_case_inst.txt')),
    ('STUDYID', 'INST_FAKE', 'DAYS_ADM_INDEX', 'DAYS_DISCH_INDEX'),
    ('ID', 'CONDITION_ID')
).tocsv(resolve('work/encounters.csv'))

preprocess(
    etl.io.csv.fromtsv(resolve('lab_case_inst.txt')),
    'STUDYID'
).tocsv(resolve('work/observations.csv'))

preprocess(
    etl.io.csv.fromtsv(resolve('med_case_inst_gpi.txt')),
    'CASE_ID'
).tocsv(resolve('work/med_dispenses.csv'))

preprocess(
    etl.io.csv.fromtsv(resolve('order_case_inst_gpi.txt')),
    'STUDYID'
).tocsv(resolve('work/med_requests.csv'))

preprocess(
    etl.io.csv.fromtsv(resolve('proc_case_inst.txt')),
    'STUDYID'
).tocsv(resolve('work/procedures.csv'))
