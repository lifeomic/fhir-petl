import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp
import random

mkdirp(resolve("work"))

selection = etl.io.csv.fromtsv(resolve("sa1cases.txt")).columns()["STUDYID"]
selection = set(random.sample(selection, 1000))

preprocess(
    etl.io.csv.fromcsv(resolve("case_demog_27065.csv")).selectin("STUDYID", selection),
    "STUDYID",
).tocsv(resolve("work/Patient.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("dx_case_inst.txt")).selectin("STUDYID", selection),
    "STUDYID",
).tocsv(resolve("work/Condition.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("lab_case_inst.txt")).selectin("STUDYID", selection),
    "STUDYID",
).tocsv(resolve("work/Observation.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("med_case_inst_gpi.txt")).selectin("CASE_ID", selection),
    "CASE_ID",
).tocsv(resolve("work/MedicationDispense.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("order_case_inst_gpi.txt")).selectin(
        "STUDYID", selection
    ),
    "STUDYID",
).tocsv(resolve("work/MedicationRequest.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("proc_case_inst.txt")).selectin("STUDYID", selection),
    "STUDYID",
).tocsv(resolve("work/Procedure.csv"))
