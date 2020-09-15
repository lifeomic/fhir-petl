import json
import petl as etl


def to_json(table, resourceType, source):
    mapper = types[resourceType]
    table = table.fieldmap({"data": mapper}, True)
    etl.io.text.totext(table, source, "utf8", template="{data}\n")
    return table


def to_timing(x):
    timing = {}
    event, code = x
    if event:
        timing["event"] = event
    if code:
        timing["code"] = to_codeable_concept(code)
    return timing


def to_codeable_concept(x):
    if isinstance(x, list):
        result = map(tuple_to_code, x)
    else:
        result = [tuple_to_code(x)]
    return {"coding": [c for c in result if c.get("code")]}


def to_ratio(x):
    numerator, denominator = x
    ratio = {}
    if numerator:
        ratio["numerator"] = to_quantity(numerator)
    if denominator:
        ratio["denominator"] = to_quantity(denominator)
    return ratio


def to_quantity(x):
    value, comparator, unit, system, code = x
    quantity = {}
    if value:
        quantity["value"] = value
    if comparator:
        quantity["comparator"] = comparator
    if unit:
        quantity["unit"] = unit
    if system:
        quantity["system"] = system
    if code:
        quantity["code"] = code
    return quantity


def to_simple_quantity(x):
    value, unit, system, code = x
    return to_quantity((value, None, unit, system, code))


def to_range(x):
    low, high = x
    range = {}
    if low:
        range["low"] = to_simple_quantity(low)
    if high:
        range["high"] = to_simple_quantity(high)
    return range


def to_ratio(x):
    numerator, denominator = x
    ratio = {}
    if numerator:
        ratio["numerator"] = to_quantity(numerator)
    if denominator:
        ratio["denominator"] = to_quantity(denominator)
    return ratio


def to_quantity(x):
    value, comparator, unit, system, code = x
    quantity = {}
    if value:
        quantity["value"] = value
    if comparator:
        quantity["comparator"] = comparator
    if unit:
        quantity["unit"] = unit
    if system:
        quantity["system"] = system
    if code:
        quantity["code"] = code
    return quantity


def to_simple_quantity(x):
    value, unit, system, code = x
    return to_quantity((value, None, unit, system, code))


def to_range(x):
    low, high = x
    range = {}
    if low:
        range["low"] = to_simple_quantity(low)
    if high:
        range["high"] = to_simple_quantity(high)
    return range


def tuple_to_code(x):
    if len(x) == 2:
        display = None
        system, code = x
    else:
        system, code, display = x

    if system and code and display:
        return {"system": system, "code": code, "display": display}
    if code and display:
        return {"code": code, "display": display}
    if system and code:
        return {"system": system, "code": code}
    if system and display:
        return {"system": system, "display": display}
    if code:
        return {"code": code}
    if display:
        return {"display": display}
    if system:
        return {"system": system}

    return None


def has(rec, field):
    return field in rec.flds and rec[field]


def to_dosage(rec):
    dosage = {}
    doseAndRate = {}
    if has(rec, "sequence"):
        dosage["sequence"] = rec["sequence"]
    if has(rec, "dosage_text"):
        dosage["text"] = rec["dosage_text"]
    if has(rec, "additionalInstruction"):
        dosage["additionalInstruction"] = to_codeable_concept(
            rec["additionalInstruction"]
        )
    if has(rec, "patientInstruction"):
        dosage["patientInstruction"] = rec["patientInstruction"]
    if has(rec, "timing"):
        dosage["timing"] = to_timing(rec["timing"])
    if has(rec, "asNeededBoolean"):
        dosage["asNeededBoolean"] = rec["asNeededBoolean"]
    if has(rec, "asNeeded"):
        dosage["asNeededCodeableConcept"] = to_codeable_concept(rec["asNeeded"])
    if has(rec, "site"):
        dosage["site"] = to_codeable_concept(rec["site"])
    if has(rec, "route"):
        dosage["route"] = to_codeable_concept(rec["route"])
    if has(rec, "method"):
        dosage["method"] = to_codeable_concept(rec["method"])
    if has(rec, "type"):
        doseAndRate["type"] = to_codeable_concept(rec["type"])
    if has(rec, "doseRange"):
        doseAndRate["doseRange"] = to_ratio(rec["doseRange"])
    if has(rec, "doseQuantity"):
        doseAndRate["doseQuantity"] = to_simple_quantity(rec["doseQuantity"])
    if has(rec, "rateRatio"):
        doseAndRate["rateRatio"] = to_ratio(rec["rateRatio"])
    if has(rec, "rateRange"):
        doseAndRate["rateRange"] = to_range(rec["rateRange"])
    if has(rec, "rateQuantity"):
        doseAndRate["rateQuantity"] = to_simple_quantity(rec["rateQuantity"])
    if doseAndRate:
        dosage["doseAndRate"] = [doseAndRate]
    if has(rec, "maxDosePerPeriod"):
        dosage["maxDosePerPeriod"] = to_ratio(rec["maxDosePerPeriod"])
    if has(rec, "maxDosePerAdministration"):
        dosage["maxDosePerAdministration"] = to_simple_quantity(
            rec["maxDosePerAdministration"]
        )
    if has(rec, "maxDosePerLifetime"):
        dosage["maxDosePerLifetime"] = to_simple_quantity(rec["maxDosePerLifetime"])
    return dosage


def to_patient(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "Patient"
    if has(rec, "subject_id"):
        result["identifier"] = [
            {
                "type": to_codeable_concept(("http://hl7.org/fhir/v2/0203", "ANON")),
                "system": "http://lifeomic.com/fhir/subject-id",
                "value": rec["subject_id"],
            }
        ]
    if has(rec, "race"):
        result["extension"] = [
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "valueCodeableConcept": to_codeable_concept(rec["race"]),
            }
        ]
    if has(rec, "ethnicity"):
        result["extension"] = [
            {
                "url": "http://hl7.org/fhir/StructureDefinition/us-core-ethnicity",
                "valueCodeableConcept": to_codeable_concept(rec["ethnicity"]),
            }
        ]
    if has(rec, "race") and has(rec, "ethnicity"):
        result["extension"] = [
            {
                "url": "http://hl7.org/fhir/StructureDefinition/us-core-ethnicity",
                "valueCodeableConcept": to_codeable_concept(rec["ethnicity"]),
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "valueCodeableConcept": to_codeable_concept(rec["race"]),
            },
        ]
    if has(rec, "marital_status"):
        result["marital_status"] = to_codeable_concept(rec["marital_status"])
    if has(rec, "gender"):
        result["gender"] = rec["gender"]
    if has(rec, "birth_date"):
        result["birthDate"] = rec["birth_date"].isoformat()
    if has(rec, "death_date"):
        result["deceasedDateTime"] = rec["death_date"].isoformat()
    if has(rec, "tag"):
        result["meta"] = {"tag": [tuple_to_code(rec["tag"])]}
    return json.dumps(result)


def to_procedure(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "Procedure"
    if has(rec, "date"):
        result["performedDateTime"] = rec["date"].isoformat()
    if has(rec, "code"):
        result["code"] = to_codeable_concept(rec["code"])
    if has(rec, "subject"):
        result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    return json.dumps(result)


def to_condition(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "Condition"
    if has(rec, "onset"):
        result["onsetDateTime"] = rec["onset"].isoformat()
    if has(rec, "asserted"):
        result["assertedDate"] = rec["asserted"].isoformat()
    if has(rec, "code"):
        result["code"] = to_codeable_concept(rec["code"])
    if has(rec, "bodySite"):
        result["bodySite"] = [to_codeable_concept(rec["bodySite"])]
    if has(rec, "severity"):
        result["severity"] = to_codeable_concept(rec["severity"])
    if has(rec, "subject"):
        result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    if has(rec, "tag"):
        result["meta"] = {"tag": [tuple_to_code(rec["tag"])]}
    return json.dumps(result)


def to_observation(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "Observation"
    if has(rec, "date"):
        result["effectiveDateTime"] = rec["date"].isoformat()
    if has(rec, "code"):
        result["code"] = to_codeable_concept(rec["code"])
    if has(rec, "subject"):
        if has(rec, "subject_display"):
            result["subject"] = {
                "reference": "Patient/" + rec["subject"],
                "display": rec["subject_display"],
            }
        else:
            result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "value"):
        value = rec["value"]
        if isinstance(value, (int, float)):
            result["valueQuantity"] = to_simple_quantity(rec["value"])
        elif isinstance(value, tuple):
            result["valueQuantity"] = to_simple_quantity(rec["value"])
        elif isinstance(value, list):
            result["valueCodeableConcept"] = to_codeable_concept(rec["value"])
        elif isinstance(value, str):
            result["valueString"] = value
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    if has(rec, "status"):
        result["status"] = rec["status"]
    return json.dumps(result)


def to_med_dispense(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "MedicationDispense"
    if has(rec, "date"):
        result["whenHandedOver"] = rec["date"].isoformat()
    if has(rec, "medication"):
        result["medicationCodeableConcept"] = to_codeable_concept(rec["medication"])
    if has(rec, "subject"):
        result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "quantity"):
        result["quantity"] = {"value": rec["quantity"]}
    if has(rec, "daysSupply"):
        result["daysSupply"] = {"value": rec["daysSupply"], "unit": "days"}
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    return json.dumps(result)


def to_med_request(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "MedicationRequest"
    if has(rec, "date"):
        result["authoredOn"] = rec["date"].isoformat()
    if has(rec, "medication"):
        result["medicationCodeableConcept"] = to_codeable_concept(rec["medication"])
    if has(rec, "subject"):
        result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    if has(rec, "status"):
        result["status"] = rec["status"]
    return json.dumps(result)


def to_med_statement(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "MedicationStatement"
    if has(rec, "nct"):
        result["extension"] = [
            {
                "url": "http://hl7.org/fhir/StructureDefinition/patient-clinicalTrial-NCT",
                "valueString": rec["nct"],
            }
        ]
    if has(rec, "subject"):
        if has(rec, "subject_display"):
            result["subject"] = {
                "reference": "Patient/" + rec["subject"],
                "display": str(rec["subject_display"]),
            }
        else:
            result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "start_date"):
        result["effectivePeriod"] = {}
        result["effectivePeriod"].update({"start": rec["start_date"].isoformat()})
    if has(rec, "end_date"):
        result["effectivePeriod"].update({"end": rec["end_date"].isoformat()})
    if has(rec, "medication"):
        result["medicationCodeableConcept"] = to_codeable_concept(rec["medication"])
    if has(rec, "status"):
        result["status"] = rec["status"]
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    if has(rec, "indication"):
        result["reasonCode"] = [to_codeable_concept(rec["indication"])]
    if has(rec, "route"):
        result["dosage"] = [{"route": to_codeable_concept(rec["route"])}]
    if has(rec, "indication"):
        result["reasonCode"] = [to_codeable_concept(rec["indication"])]
    if has(rec, "route"):
        result["dosage"] = [{"route": to_codeable_concept(rec["route"])}]
    return json.dumps(result)


def to_med_administration(rec):
    result = {}
    result["id"] = rec["id"]
    result["resourceType"] = "MedicationAdministration"
    if has(rec, "status"):
        result["status"] = rec["status"]
    if has(rec, "subject"):
        result["subject"] = {"reference": "Patient/" + rec["subject"]}
    if has(rec, "medication"):
        result["medicationCodeableConcept"] = to_codeable_concept(rec["medication"])
    if has(rec, "start_date"):
        result["effectivePeriod"] = {}
        result["effectivePeriod"].update({"start": rec["start_date"].isoformat()})
    if has(rec, "end_date"):
        result["effectivePeriod"].update({"end": rec["end_date"].isoformat()})
    if has(rec, "note"):
        result["note"] = [{"text": rec["note"]}]
    dosage = to_dosage(rec)
    if "doseAndRate" in dosage:
        dosage.update(dosage.pop("doseAndRate")[0])
    if "doseQuantity" in dosage:
        dosage["dose"] = dosage.pop("doseQuantity")
    result["dosage"] = dosage
    return json.dumps(result)


types = {
    "Procedure": to_procedure,
    "Patient": to_patient,
    "Condition": to_condition,
    "Observation": to_observation,
    "MedicationDispense": to_med_dispense,
    "MedicationRequest": to_med_request,
    "MedicationStatement": to_med_statement,
    "MedicationAdministration": to_med_administration,
}
