from datetime import date
import json
import petl as etl
import fhir_petl.fhir as fhir

def test_to_ratio():
    numerator_tuple = (100, '<', 'mL', 'http://unitsofmeasure.org', 'mL')
    denominator_tuple = (2, '>', 'h', 'http://unitsofmeasure.org', 'h')
    ratio_tuple = (numerator_tuple, denominator_tuple)
    ratio_dict = fhir.to_ratio(ratio_tuple)
    assert ratio_dict['numerator'] == {'value': 100, 'comparator': '<', 'unit': 'mL', 'system': 'http://unitsofmeasure.org', 'code': 'mL'}
    assert ratio_dict['denominator'] == {'value': 2, 'comparator': '>', 'unit': 'h', 'system': 'http://unitsofmeasure.org', 'code': 'h'}

def test_to_quantity():
    quantity_tuple = (100, '<', 'mL', 'http://unitsofmeasure.org', 'mL')
    quantity_dict = fhir.to_quantity(quantity_tuple)
    assert quantity_dict == {'value': 100, 'comparator': '<', 'unit': 'mL', 'system': 'http://unitsofmeasure.org', 'code': 'mL'}

def test_to_simple_quantity():
    simple_quantity_tuple = (2, 'h', 'http://unitsofmeasure.org', 'h')
    simple_quantity_dict = fhir.to_simple_quantity(simple_quantity_tuple)
    assert simple_quantity_dict == {'value': 2, 'unit': 'h', 'system': 'http://unitsofmeasure.org', 'code': 'h'}

def test_to_range():
    low_tuple = (2, 'h', 'http://unitsofmeasure.org', 'h')
    high_tuple = (4, 'h', 'http://unitsofmeasure.org', 'h')
    range_tuple = (low_tuple, high_tuple)
    range_dict = fhir.to_range(range_tuple)
    assert range_dict['low'] == {'value': 2, 'unit': 'h', 'system': 'http://unitsofmeasure.org', 'code': 'h'}
    assert range_dict['high'] == {'value': 4, 'unit': 'h', 'system': 'http://unitsofmeasure.org', 'code': 'h'}

def test_to_dosage():
    header = ['sequence', 'dosage_text', 'additionalInstruction', 'patientInstruction', 'timing',
              'asNeededBoolean', 'route', 'method', 'type', 'doseQuantity', 'rateQuantity', 'maxDosePerLifetime']
    data = [1, 'i po qd cf', ('http://snomed.info/sct', '311504000', 'With or after food'), 'Once per day with food', ('18:00:00', None),
            'false', ('http://snomed.info/sct', '26643006', 'Oral Route (qualifier value)'), ('http://snomed.info/sct', '421521009', 'Swallow'),
            ('http://terminology.hl7.org/CodeSystem/dose-rate-type', 'ordered', 'Ordered'), (100, 'mg', 'http://unitsofmeasure.org', 'mg'),
            (24, 'h', 'http://unitsofmeasure.org', 'h'), (5, 'g', 'http://unitsofmeasure.org', 'g')]
    dosage_table = etl.util.base.Record(data, header)
    dosage = fhir.to_dosage(dosage_table)
    assert dosage['sequence'] == 1
    assert dosage['text'] == 'i po qd cf'
    assert dosage['additionalInstruction'] == {'coding': [{'system': 'http://snomed.info/sct', 'code': '311504000', 'display': 'With or after food'}]}
    assert dosage['timing'] == {'event': '18:00:00'}
    assert dosage['asNeededBoolean'] == 'false'
    assert dosage['route'] == {'coding': [{'system': 'http://snomed.info/sct', 'code': '26643006', 'display': 'Oral Route (qualifier value)'}]}
    assert dosage['method'] == {'coding': [{'system': 'http://snomed.info/sct', 'code': '421521009', 'display': 'Swallow'}]}
    assert dosage['doseAndRate'][0]['type'] == {'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/dose-rate-type',
                                                            'code': 'ordered', 'display': 'Ordered'}]}
    assert dosage['doseAndRate'][0]['doseQuantity'] == {'value': 100, 'unit': 'mg', 'system': 'http://unitsofmeasure.org', 'code': 'mg'}
    assert dosage['doseAndRate'][0]['rateQuantity'] == {'value': 24, 'unit': 'h', 'system': 'http://unitsofmeasure.org', 'code': 'h'}
    assert dosage['maxDosePerLifetime'] == {'value': 5, 'unit': 'g', 'system': 'http://unitsofmeasure.org', 'code': 'g'}

def test_to_med_administration():
    header = ['id', 'status', 'subject', 'medication', 'start_date', 'end_date', 'note', 'dosage_text', 'route', 'rateRatio']
    data = ['e1aa3a08-5c36-49cf-96e4-dcca7c1b7a50', 'completed', '071f8ae4-52fd-4f2d-8090-60d1ef3a4452',
            ('http://hl7.org/fhir/sid/ndc', '49884046905', 'Ibuprofen Tab 800 MG'), date(2020, 5, 5), date(2020, 5, 7),
            'Test note', 'Test dosage text', ('http://snomed.info/sct', '26643006', 'Oral Route (qualifier value)'),
            ((100, '<', 'mg', 'http://unitsofmeasure.org', 'mg'), (24, '>', 'h', 'http://unitsofmeasure.org', 'h'))]
    administration_table = etl.util.base.Record(data, header)
    admin = json.loads(fhir.to_med_administration(administration_table))
    assert admin['id'] == 'e1aa3a08-5c36-49cf-96e4-dcca7c1b7a50'
    assert admin['status'] == 'completed'
    assert admin['subject'] == {'reference' : 'Patient/071f8ae4-52fd-4f2d-8090-60d1ef3a4452'}
    assert admin['medicationCodeableConcept'] == {'coding': [{'system': 'http://hl7.org/fhir/sid/ndc', 'code': '49884046905',
                                                              'display': 'Ibuprofen Tab 800 MG'}]}
    assert admin['effectivePeriod'] == {'start': '2020-05-05', 'end': '2020-05-07'}
    assert admin['note'] == [{'text': 'Test note'}]
    assert admin['dosage']['text'] == 'Test dosage text'
    assert admin['dosage']['route'] == {'coding': [{'system': 'http://snomed.info/sct',
                                                    'code': '26643006', 'display': 'Oral Route (qualifier value)'}]}
    assert admin['dosage']['rateRatio'] == {'numerator': {'value': 100, 'comparator': '<', 'unit': 'mg',
                                                          'system': 'http://unitsofmeasure.org', 'code': 'mg'},
                                            'denominator': {'value': 24, 'comparator': '>', 'unit': 'h',
                                                            'system': 'http://unitsofmeasure.org', 'code': 'h'}}
