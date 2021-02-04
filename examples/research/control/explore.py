import petl as etl
from fhir_petl.util import resolve

observations = etl.io.csv.fromcsv(resolve("work/Observation.csv"))
patients = etl.io.csv.fromcsv(resolve("work/Patient.csv"))
procedures = etl.io.csv.fromcsv(resolve("work/Procedure.csv"))
conditions = etl.io.csv.fromcsv(resolve("work/Condition.csv"))
requests = etl.io.csv.fromcsv(resolve("work/MedicationRequest.csv"))
dispenses = etl.io.csv.fromcsv(resolve("work/MedicationDispense.csv"))

print(requests.look(10))
