import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp

mkdirp(resolve('work'))

preprocess(
    etl.io.csv.fromcsv(resolve('Table_1_Demographics_New_Cohorts.csv')),
    resolve('work/Patient.csv'))

preprocess(
    etl.io.csv.fromcsv(resolve('Diagnoses.csv')),
    resolve('work/Condition.csv'))

preprocess(
    etl.io.csv.fromcsv(resolve('fairbanks_cv.dedup.csv')),
    resolve('work/Observation.csv'))

preprocess(
    etl.io.csv.fromcsv(resolve('Prescriptions.csv')),
    resolve('work/MedicationRequest.csv'))

preprocess(
    etl.io.csv.fromcsv(resolve('Procedures.csv')),
    resolve('work/Procedure.csv'))
