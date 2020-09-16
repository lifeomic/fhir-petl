import petl as etl
from fhir_petl.util import preprocess, resolve, mkdirp
import random

mkdirp(resolve("work"))

selection = etl.io.csv.fromtsv(resolve("sa1controls.txt")).columns()["control_id"]
selection = set(random.sample(selection, 1000))

preprocess(
    etl.io.csv.fromtsv(resolve("controls.txt")).selectin("control_id", selection),
    "control_id",
).tocsv(resolve("work/Patient.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("dx_control_inst.txt")).selectin(
        "CONTROL_ID", selection
    ),
    "CONTROL_ID",
).tocsv(resolve("work/Condition.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("lab_control_inst.txt")).selectin(
        "CONTROL_ID", selection
    ),
    "CONTROL_ID",
).tocsv(resolve("work/Observation.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("med_control_inst_gpi.txt")).selectin(
        "CONTROL_ID", selection
    ),
    "CONTROL_ID",
).tocsv(resolve("work/MedicationDispense.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("order_control_inst_gpi.txt")).selectin(
        "CONTROL_ID", selection
    ),
    "CONTROL_ID",
).tocsv(resolve("work/MedicationRequest.csv"))

preprocess(
    etl.io.csv.fromtsv(resolve("proc_control_inst.txt")).selectin(
        "CONTROL_ID", selection
    ),
    "CONTROL_ID",
).tocsv(resolve("work/Procedure.csv"))
