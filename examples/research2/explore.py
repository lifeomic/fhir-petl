import petl as etl
from fhir_petl.util import resolve, mkdirp, number, year, dateparser

observations = (etl.io.csv.fromcsv(resolve('work/Observation.csv')))
patients = (etl.io.csv.fromcsv(resolve('work/Patient.csv')))
procedures = (etl.io.csv.fromcsv(resolve('work/Procedure.csv')))

print(observations.look(100))