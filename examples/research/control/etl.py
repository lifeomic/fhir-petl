from datetime import timedelta
import petl as etl
from fhir_petl.fhir import to_json
from fhir_petl.util import resolve, mkdirp, number, join, year, dateparser, ISOFormat


def map_race(race):
    return {
        "AMERICAN INDIAN AND ALASKA NATIVE": (
            "http://hl7.org/fhir/v3/Race",
            "1002-5",
            "American Indian or Alaska Native",
        ),
        "ASIAN": ("http://hl7.org/fhir/v3/Race", "2028-9", "Asian"),
        "BLACK OR AFRICAN AMERICAN": (
            "http://hl7.org/fhir/v3/Race",
            "2054-5",
            "Black or African American",
        ),
        "HISPANIC OR LATINO": ("http://hl7.org/fhir/v3/Race", "2106-3", "White"),
        "WHITE": ("http://hl7.org/fhir/v3/Race", "2106-3", "White"),
        "NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER": (
            "http://hl7.org/fhir/v3/Race",
            "2076-8",
            "Native Hawaiian or Other Pacific Islander",
        ),
    }.get(race, None)


def index_date(rec):
    birth = number(rec["BIRTH_YR"])
    index_age = number(rec["INDEX_AGE"])
    index_date = str(birth + index_age)
    return dateparser("%Y", ISOFormat.DAY)(index_date)


patients = etl.io.csv.fromcsv(resolve("work/Patient.csv")).fieldmap(
    {
        "id": "ID",
        "CONTROL_ID": "control_id",
        "subject_id": ("control_id", lambda x: "CONTROL-" + x),
        "race": ("RACE", map_race),
        "gender": ("SEX", {"F": "female", "M": "male"}),
        "birth_date": ("BIRTH_YR", year),
        "index_date": index_date,
        "tag": lambda rec: ("subject-type", "control"),
    },
    True,
)

index = patients.cut("CONTROL_ID", "id", "index_date").rename("id", "subject")

procedures = (
    etl.io.csv.fromcsv(resolve("work/Procedure.csv"))
    .hashjoin(index, lkey="CONTROL_ID", rkey="CONTROL_ID")
    .fieldmap(
        {
            "id": "ID",
            "date": lambda rec: rec["index_date"]
            + timedelta(int(rec["DAYS_VIS_INDEX"])),
            "code": lambda rec: (
                "http://www.ama-assn.org/go/cpt",
                rec["PROC_CODE"],
                rec["NAME"].strip('" '),
            ),
            "subject": "subject",
        },
        True,
    )
)

conditions = (
    etl.io.csv.fromcsv(resolve("work/Condition.csv"))
    .hashjoin(index, lkey="CONTROL_ID", rkey="CONTROL_ID")
    .select("DX_CODE", lambda x: x)
    .fieldmap(
        {
            "id": "ID",
            "onset": lambda rec: rec["index_date"]
            + timedelta(int(rec["DAYS_ADM_INDEX"])),
            "code": lambda rec: ("http://hl7.org/fhir/sid/icd-9-cm", rec["DX_CODE"]),
            "note": lambda rec: join(
                rec["CARE_SETTING_TEXT"], rec["LOCATION_POINT_OF_CARE"]
            ),
            "subject": "subject",
        },
        True,
    )
)

observations = (
    etl.io.csv.fromcsv(resolve("work/Observation.csv"))
    .hashjoin(index, lkey="CONTROL_ID", rkey="CONTROL_ID")
    .fieldmap(
        {
            "id": "ID",
            "date": lambda rec: rec["index_date"]
            + timedelta(int(rec["DAYS_VIS_INDEX"])),
            "code": lambda rec: ("lab-text", rec["NAME"], rec["NAME"]),
            "value": lambda rec: number(rec["RESULT_VALUE"])
            if rec["RESULT_VALUE"]
            else (rec["CODED_NAME"] or None),
            "subject": "subject",
        },
        True,
    )
    .select("value", lambda x: x)
)


def medications(rec):
    group = rec["DRUG_GROUP"].strip("*")
    clazz = rec["DRUG_CLASS"].strip("*")
    return [
        ("http://hl7.org/fhir/sid/ndc", rec["NDC_CODE"], rec["DRUG_NAME"]),
        ("urn:oid:2.16.840.1.113883.6.68", rec["GPI_CODE"], rec["DRUG_NAME"]),
        ("drug-class", clazz, clazz),
        ("drug-group", group, group),
    ]


med_dispenses = (
    etl.io.csv.fromcsv(resolve("work/MedicationDispense.csv"))
    .hashjoin(index, lkey="CONTROL_ID", rkey="CONTROL_ID")
    .fieldmap(
        {
            "id": "ID",
            "date": lambda rec: rec["index_date"]
            + timedelta(int(rec["DAYS_VIS_INDEX"])),
            "medication": medications,
            "quantity": ("DISPENSE_AMOUNT", number),
            "daysSupply": ("NUMBER_OF_DAYS_SUPPLY", number),
            "subject": "subject",
        },
        True,
    )
)


def medications2(rec):
    group = rec["DRUG_GROUP"].strip("*")
    clazz = rec["DRUG_CLASS"].strip("*")
    return [
        ("http://hl7.org/fhir/sid/ndc", rec["NDC"], rec["ORDER_NAME"]),
        ("urn:oid:2.16.840.1.113883.6.68", rec["GPI"], rec["ORDER_NAME"]),
        ("drug-class", clazz, clazz),
        ("drug-group", group, group),
    ]


med_requests = (
    etl.io.csv.fromcsv(resolve("work/MedicationRequest.csv"))
    .hashjoin(index, lkey="CONTROL_ID", rkey="CONTROL_ID")
    .fieldmap(
        {
            "id": "ID",
            "date": lambda rec: rec["index_date"]
            + timedelta(int(rec["DAYS_ORDER_INDEX"])),
            "medication": medications2,
            "subject": "subject",
        },
        True,
    )
)


mkdirp(resolve("fhir"))
to_json(patients, "Patient", resolve("fhir/Patient.json"))
to_json(procedures, "Procedure", resolve("fhir/Procedure.json"))
to_json(conditions, "Condition", resolve("fhir/Condition.json"))
to_json(observations, "Observation", resolve("fhir/Observation.json"))
to_json(med_dispenses, "MedicationDispense", resolve("fhir/MedicationDispense.json"))
to_json(med_requests, "MedicationRequest", resolve("fhir/MedicationRequest.json"))
