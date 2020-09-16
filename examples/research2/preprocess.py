import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp

mkdirp(resolve("work"))

preprocess(etl.io.csv.fromcsv(resolve("Table_1_Demographics_New_Cohorts.csv"))).tocsv(
    resolve("work/Patient.csv")
)

preprocess(etl.io.csv.fromcsv(resolve("Diagnoses.csv"))).tocsv(
    resolve("work/Condition.csv")
)

preprocess(etl.io.csv.fromcsv(resolve("fairbanks_cv.dedup.csv"))).tocsv(
    resolve("work/Observation.csv")
)

preprocess(etl.io.csv.fromcsv(resolve("Prescriptions.csv"))).tocsv(
    resolve("work/MedicationRequest.csv")
)

preprocess(etl.io.csv.fromcsv(resolve("Procedures.csv"))).tocsv(
    resolve("work/Procedure.csv")
)
