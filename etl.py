from datetime import timedelta
import petl as etl
import math
from fhir_petl.fhir import to_json
from fhir_petl.util import resolve, mkdirp, number, year, dateparser, ISOFormat
from datetime import datetime

date = dateparser("%m/%d/%y", ISOFormat.DAY)


def map_race(race):
    return {
        "Asian": ("http://hl7.org/fhir/v3/Race", "2028-9", "Asian"),
        "Black": ("http://hl7.org/fhir/v3/Race", "2054-5", "Black or African American"),
        "African American": (
            "http://hl7.org/fhir/v3/Race",
            "2054-5",
            "Black or African American",
        ),
        "White": ("http://hl7.org/fhir/v3/Race", "2106-3", "White"),
        "Other": ("http://hl7.org/fhir/v3/Race", "2131-1", "Other Race"),
        "Unknown": ("http://hl7.org/fhir/v3/NullFlavor", "UNK", "Unknown"),
        "Native American / Alaskan": (
            "http://hl7.org/fhir/v3/Race",
            "1002-5",
            "American Indian or Alaska Native",
        ),
        "Native Hawaiian / Pacific Islander": (
            "http://hl7.org/fhir/v3/Race",
            "2076-8",
            "Native Hawaiian or Other Pacific Islander",
        ),
    }.get(race, None)


def map_ethnicity(ethnicity):
    return {
        "Yes": ("http://hl7.org/fhir/v3/Ethnicity", "2135-2", "Hispanic or Latino"),
        "No": ("http://hl7.org/fhir/v3/Ethnicity", "2186-5", "Not Hispanic or Latino"),
        "NOT Hispanic or Latino": (
            "http://hl7.org/fhir/v3/Ethnicity",
            "2186-5",
            "Not Hispanic or Latino",
        ),
    }.get(ethnicity, None)


def map_ynunk(ynunk_value):
    return {
        "Unknown": ("http://hl7.org/fhir/v3/NullFlavor", "UNK", "Unknown"),
        "Yes": ("http://terminology.hl7.org/CodeSystem/v2-0136", "Y", "Yes"),
        "No": ("http://terminology.hl7.org/CodeSystem/v2-0136", "N", "No"),
        "Other": ("http://snomed.info/sct", "74964007", "Other"),
        "Unclassified": ("http://snomed.info/sct", "1491000", "Unclassified"),
        "None": ("http://hl7.org/fhir/v3/NullFlavor", "NI", "No Information"),
        -1: ("http://hl7.org/fhir/v3/NullFlavor", "UNK", "Unknown"),
        "-1": ("http://hl7.org/fhir/v3/NullFlavor", "UNK", "Unknown"),
        "Inconclusive": ("http://snomed.info/sct", "419984006", "Inconclusive"),
        "Not used": ("http://snomed.info/sct", "262009000", " Not used"),
    }.get(ynunk_value, None)


def map_marital_status(marital_status):
    return {
        "Married": ("http://hl7.org/fhir/v3/MaritalStatus", "M", "Married"),
        "Single": (
            "http://hl7.org/fhir/v3/Race",
            "U",
            "unmarried",
        ),
        "Divorced": ("http://hl7.org/fhir/v3/MaritalStatus", "D", "Divorced"),
        "Widowed": ("http://hl7.org/fhir/v3/MaritalStatus", "W", "Widowed"),
        "-1": ("http://hl7.org/fhir/v3/NullFlavor", "UNK", "unknown"),
        "None": (
            "http://hl7.org/fhir/v3/NullFlavor",
            "UNK",
            "unknown",
        ),
    }.get(marital_status, None)


def sample_date(text):
    parser = dateparser("%m/%d/%y %H:%M", ISOFormat.DAY)
    return parser(text)


def birth_date(rec):
    sample_age = timedelta(int(rec["Age (Sample)"]) * 365.25)
    return sample_date(rec["SAMPLE_DATE"]) - sample_age


def parse_date(text, month=None, day=None, complete_date_bool=False):

    if len(text) == 5:
        dt = datetime.fromordinal(
            datetime(1900, 1, 1).toordinal() + int(text) - 2
        ).date()
        parser = dateparser("%Y-%m-%d", ISOFormat.DAY)
        date_string = str(dt)

    elif len(text) < 4:
        return None

    elif month and day and text:
        parser = dateparser("%m/%d/%Y", ISOFormat.DAY)
        date_string = "{0}/{1}/{2}".format(month, day, text)

    elif day and text:
        parser = dateparser("%Y", ISOFormat.YEAR)
        date_string = "{0}".format(text)

    elif month and text and not day:
        parser = dateparser("%m/%Y", ISOFormat.MONTH)
        date_string = "{0}/{1}".format(month, text)

    elif not complete_date_bool:
        parser = dateparser("%Y", ISOFormat.YEAR)
        date_string = "{0}".format(text)
    else:
        parser = dateparser("%m/%d/%Y", ISOFormat.DAY)
        date_string = "{0}".format(text)

    return parser(date_string)


patients = etl.io.csv.fromcsv(resolve("work/Patient_ktb_updated.csv")).fieldmap(
    {
        "id": "ID",
        "subject_id": "SID",
        "SID": "SID",
        "race": ("RACE", map_race),
        "ethnicity": ("ETHNICITY", map_ethnicity),
        "marital_status": ("MARITAL_STATUS", map_marital_status),
        # "gender": ("GENDER", {"F": "female", "M": "male"}),
        # "birth_date": ("BIRTH_DATE", parse_date),
        # 'death_date': ('DEATH_YR', year),
        # 'sample_date': ('SAMPLE_DATE', sample_date),
        # 'tag': ('Cohort', lambda cohort: ('cohort', cohort.upper()))
    },
    True,
)

index = patients.cut("SID", "id").rename("id", "subject")


def proc_code(rec):
    if rec["PROC_SYS_ID"] == 1:
        return (
            "http://www.ama-assn.org/go/cpt",
            rec["PROC_CODE"],
            rec["PROC_NAME"].strip('" '),
        )

    return (
        "http://hl7.org/fhir/sid/icd-9-cm",
        rec["PROC_CODE"],
        rec["PROC_NAME"].strip('" '),
    )


# procedures = (etl.io.csv.fromcsv(resolve('work/Procedure.csv'))
#               .hashjoin(index, lkey='SID', rkey='SID')
#               .fieldmap({
#                   'id': 'ID',
#                   'date': lambda rec: date(rec['PROC_DATE'] or rec['ARRIVE_DATE'] or rec['DISCHARGE_DATE']),
#                   'code': proc_code,
#                   'subject': 'subject'
#                   'note': 'ENC_TYPE'
#               }, True))


def cv_code(rec):
    codes = []

    # if rec["LOINC_CODE"]:
    #     codes.append(("http://loinc.org", rec["LOINC_CODE"], rec["CV_NAME"]))
    # elif rec["CV_CODE"]:
    #     codes.append(("http://regenstrief.org/cv_code", rec["CV_CODE"], rec["CV_NAME"]))

    if rec["VALUE_CODE"] and "CODE_SYSTEM" in rec:
        if rec["CODE_SYSTEM"] == "LOINC":
            codes.append(
                (
                    "http://loinc.org",  # system
                    rec["VALUE_CODE"].strip(),  # code
                    rec["CODE_DESC"].strip(),  # display
                )
            )
        elif rec["CODE_SYSTEM"] == "custom":
            codes.append(
                (
                    "http://lifeomic.com/ktb/" + rec["VALUE_CODE"].strip(),
                    rec["VALUE_CODE"].strip(),
                    rec["CODE_DESC"].strip(),
                )
            )
    elif rec["VALUE_CODE"]:
        codes.append(
            (
                "http://lifeomic.com/ktb/" + rec["VALUE_CODE"].strip(),
                rec["VALUE_CODE"].strip(),
                rec["CODE_DESC"].strip(),
            )
        )

    return codes


def cv_value(rec):
    values = []
    # if rec["CV_RESULT_NUMERIC"]:
    #     return number(rec["CV_RESULT_NUMERIC"])
    if rec["VALUE_TYPE"] == "valueQuantity":
        if rec["VALUE"] not in [0, "0", "", None]:
            return (
                number(rec["VALUE"].strip()),
                rec["UCUM_TYPE"],
                "http://unitsofmeasure.org",
                rec["UCUM_CODE"],
            )
        else:
            return number(rec["VALUE"])
    elif rec["VALUE_TYPE"] == "vcc_custom":
        codified_value = rec["VALUE"].strip().replace(" ", "_").upper()
        values.append(
            (
                "http://lifeomic.com/ktb/value/" + codified_value,  # system
                codified_value,  # code
                rec["VALUE"],  # display
            )
        )
        return values
    elif rec["VALUE_TYPE"] == "vcc_custom_ynunk":
        return map_ynunk(rec["VALUE"])

    else:
        return rec["VALUE"]


observations = (
    etl.io.csv.fromcsv(resolve("work/Observation_bmi_gs.csv"), encoding="utf-8-sig")
    .hashjoin(index, lkey="SID", rkey="SID")
    .fieldmap(
        {
            "id": "ID",
            "date": ("VALUE_DATE", parse_date),
            "code": cv_code,
            "value": cv_value,
            "subject": "subject",
            "status": "STATUS",
            "subject_display": "SID",
        },
        True,
    )
    .selecttrue("value")
)

# observations = (
#     etl.io.csv.fromcsv(resolve("work/Observation_ktb2.csv"), encoding="utf-8-sig")
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "date": ("VALUE_DATE", parse_date),
#             "code": cv_code,
#             "value": cv_value,
#             "subject": "subject",
#             "status": "STATUS",
#             "subject_display": "SID",
#         },
#         True,
#     )
#     .selecttrue("value")
# )

# observations = (
#     etl.io.csv.fromcsv(resolve("work/Observation_ktb2.csv"), encoding="utf-8-sig")
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "date": ("VALUE_DATE", parse_date),
#             "code": cv_code,
#             # "value": ("VALUE", map_ynunk),
#             "value_type": "VALUE_TYPE",
#             "subject": "subject",
#             "status": "STATUS",
#             "subject_display": "SID",
#         },
#         True,
#     )
#     .selecttrue("date")
# )

# date_observations = etl.select(
#     observations, "value_type", lambda v: v == "effectiveDatetime"
# )

# observations = (
#     etl.io.csv.fromcsv(resolve("work/ktb_Obs_valueQuantity.csv"), encoding="utf-8-sig")
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "date": ("VALUE_DATE", parse_date),
#             "code": cv_code,
#             "value": cv_value,
#             "subject": "subject",
#             "status": "STATUS",
#             "subject_display": "SID",
#         },
#         True,
#     )
#     .selecttrue("value")
# )

# observations = (
#     etl.io.csv.fromcsv(resolve("work/ktb_Obs_vcc.csv"), encoding="utf-8-sig")
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "date": ("VALUE_DATE", parse_date),
#             "code": cv_code,
#             "value": cv_value,
#             "subject": "subject",
#             "status": "STATUS",
#             "subject_display": "SID",
#         },
#         True,
#     )
#     .selecttrue("value")
# )


def dx_code(rec):
    if rec["DX_SYS_ID"] == "9":
        return ("http://hl7.org/fhir/sid/icd-9-cm", rec["DX_CODE"], rec["DX_NAME"])

    return ("http://hl7.org/fhir/sid/icd-10", rec["DX_CODE"], rec["DX_NAME"])


def map_dx(rec):
    if rec["CONDITION_CODE"]:
        return (
            "http://lifeomic.com/fhir/sid/dx",
            rec["CONDITION_CODE"].replace(" ", "_"),
            rec["CONDITION_NAME"],
        )


def map_site(site):
    return {
        "Brain": (
            "http://snomed.info/sct",
            "12738006",
            "Brain structure (body structure)",
        ),
    }.get(stage, None)


def map_stage(stage):
    return {
        "Stage I": ("http://snomed.info/sct", "258215001", "Stage 1 (qualifier value)"),
        "Stage II": (
            "http://snomed.info/sct",
            "258219007",
            "Stage 2 (qualifier value)",
        ),
        "Stage III": (
            "http://snomed.info/sct",
            "258224005",
            "Stage 3 (qualifier value)",
        ),
        "Stage IV": (
            "http://snomed.info/sct",
            "258228008",
            "Stage 4 (qualifier value)",
        ),
        "Unknown": ("http://snomed.info/sct", "261665006", "Unknown (qualifier value)"),
    }.get(stage, None)


# conditions = (
#     etl.io.csv.fromcsv(resolve("work/Condition_ktb.csv"))
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "onset": ("DATE_OF_DX", parse_date),
#             "code": map_dx,
#             #   'bodySite': ('SITE', map_site),
#             #   'severity': ('STAGE', map_stage),
#             "subject": "subject",
#             #   'tag': ()
#         },
#         True,
#     )
#     .selecttrue("code")
# )

# med_requests = (etl.io.csv.fromcsv(resolve('work/MedicationRequest.csv'))
#                 .hashjoin(index, lkey='SID', rkey='SID')
#                 .fieldmap({
#                     'id': 'ID',
#                     'date': ('ORDER_DATE', date),
#                     'medication': lambda rec: ('http://hl7.org/fhir/sid/ndc', rec['NDC'], rec['DRUG_NAME']),
#                     'subject': 'subject',
#                     'status': ('FILL_COMPLETE', lambda complete: 'completed' if complete == '1' else None)
#                 }, True))


def map_rx_route(route):
    return {
        "OP": ("http://snomed.info/sct", "26643006", "Oral Route"),
        "IV": (
            "http://snomed.info/sct",
            "47625008",
            "Intravenous route (qualifier value)",
        ),
    }.get(route, None)


def map_status_reason(route):
    return {
        "completed": ("http://snomed.info/sct", "182834008", "Drug course completed"),
        "disease progression": (
            "http://snomed.info/sct",
            "395007004",
            "Medication stopped - ineffective",
        ),
        "patient choice": (
            "http://snomed.info/sct",
            "182844005",
            "Doctor stopped drugs - patient dislikes",
        ),
        "physician choice": (
            "http://snomed.info/sct",
            "182846007",
            "Doctor stopped drugs - medical aim achieved",
        ),
        "toxicities": (
            "http://snomed.info/sct",
            "395008009",
            "Medication stopped - contra-indication",
        ),
    }.get(route, None)


def map_indication(indication):
    return {
        "Adjuvant": (
            "http://snomed.info/sct",
            "373846009",
            "Adjuvant - intent (qualifier value)",
        ),
        "Neoadjuvant": (
            "http://snomed.info/sct",
            "373847000",
            "Neo-adjuvant - intent (qualifier value)",
        ),
        "Adjuvant - on Clinical Trial": (
            "http://snomed.info/sct",
            "373846009",
            "Adjuvant - intent (qualifier value)",
        ),
        "Neoadjuvant - on Clinical Trial": (
            "http://snomed.info/sct",
            "373847000",
            "Neo-adjuvant - intent (qualifier value)",
        ),
    }.get(indication, None)


# med_statements = (
#     etl.io.csv.fromcsv(
#         resolve("work/MedicationStatement_ktb.csv"), encoding="utf-8-sig"
#     )
#     .hashjoin(index, lkey="SID", rkey="SID")
#     .fieldmap(
#         {
#             "id": "ID",
#             "subject_display": "SID",
#             "medication": lambda rec: (
#                 "http://lifeomic.com/fhir/ktb/rx",
#                 rec["DRUG_CODE"],
#                 rec["DRUG_NAME"],
#             ),
#             # "route": ("ROUTE", map_rx_route),
#             "subject": "subject",
#             # # "status": (
#             #     "ONGOING",
#             #     lambda active: "active"
#             #     if active == "TRUE"
#             #     else "completed"
#             #     if active == "FALSE"
#             #     else None,
#             # ),
#             # "note": "STATUS_REASON",
#             "start_date": ("START_DATE", parse_date),
#             # "indication": ("INDICATION", map_indication),
#             # "end_date": ("END_DATE", date),
#             # "nct": ("NCT"),
#         },
#         True,
#     )
# )

mkdirp(resolve("fhir"))
# to_json(patients, "Patient", resolve("fhir/Patient_ktb_updated.json"))
# to_json(procedures, 'Procedure', resolve('fhir/Procedure.json'))
to_json(observations, "Observation", resolve("fhir/Observation_bmi_gs.json"))
# to_json(conditions, "Condition", resolve("fhir/Condition_ktb.json"))
# to_json(med_requests, 'MedicationRequest', resolve('fhir/MedicationRequest.json'))
# to_json(
#     med_statements, "MedicationStatement", resolve("fhir/MedicationStatement_ktb.json")
# )
