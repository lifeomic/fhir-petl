import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp

mkdirp(resolve("work"))

# preprocess(etl.io.csv.fromcsv(resolve("Patients_ktb_updated.csv"))).tocsv(
#     resolve("work/Patient_ktb_updated.csv")
# )

# preprocess(etl.io.csv.fromcsv(resolve("Updated_Medication_final.csv"))).tocsv(
#     resolve("work/MedicationStatement_ktb.csv")
# )

preprocess(etl.io.csv.fromcsv(resolve("Condition_ktb.csv"))).tocsv(
    resolve("work/Condition_ktb.csv")
)

# preprocess(etl.io.csv.fromcsv(resolve("Observation_bmi_gs.csv"))).tocsv(
#     resolve("work/Observation_bmi_gs.csv")
# )

# preprocess(
#     etl.io.csv.fromcsv(resolve('Beautiful_MedicationRequests.csv'))
# ).tocsv(resolve('work/MedicationRequest.csv'))

# preprocess(
#     etl.io.csv.fromcsv(resolve('Beatufiul_Procedures.csv'))
# ).tocsv(resolve('work/Procedure.csv'))
