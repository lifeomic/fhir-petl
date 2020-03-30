'''
CSV FHIR-IFY
Author: Kevin Wood
Purpose: Transform flat file into fhir_petl-ready csv
'''
import csv
import data_mappings as dm
from fhir_petl.util import resolve



def get_current_row(row_info, join_key_value):
    '''check if key is in the csv'''
    if join_key_value in row_info:
        # print(row_info[join_key_value])
        return row_info[join_key_value]
    return {}

def get_row_indices(all_headers, to_keep):
    '''get the indices of each row'''
    row_indices = []
    for outer_header in to_keep:
        if isinstance(outer_header, str):
            row_index = all_headers.index(outer_header)
            if row_index > -1:
                row_indices.append(row_index)
        else:
            for inner_header in outer_header:
                row_index = all_headers.index(inner_header)
                if row_index > -1:
                    row_indices.append(row_index)
    return row_indices

def generate_row_info(in_files, join_key):
    '''generate information about the row'''
    row_info = {}
    for file_name in in_files.keys():
        with open(resolve(file_name), mode='r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            headers = next(reader, None)
            headers = [x.strip(' ') for x in headers]
            if 'columns_to_keep' in in_files[file_name]:
                headers_to_keep = in_files[file_name]['columns_to_keep']
                if in_files[file_name]['skip_first_line']:
                    next(reader, None)
            else:
                headers_to_keep = in_files[file_name]['multiple_columns_to_keep']

            row_indices = get_row_indices(headers, headers_to_keep)

            for row in reader:
                join_key_col_index = headers.index(join_key)
                current_row_info = get_current_row(row_info, row[join_key_col_index])

                for index in row_indices:
                    current_row_info[headers[index]] = row[index]

                row_info[row[join_key_col_index]] = current_row_info

    return row_info

def transform_single_row_info(row, target_mappings):
    '''transform the row info for a single row into a row mapped to target keys'''
    mapped_row = {}
    options_for_none = ['None', 'NA', 'N/A', '?', '#VALUE!']
    for key in target_mappings.keys():
        mapping_options = target_mappings[key]
        if len(mapping_options) == 1 and mapping_options[0] in row:
            mapped_row[key] = row[mapping_options[0]].strip()
        elif len(mapping_options) > 1:
            for option in reversed(mapping_options):
                if option in row and row[option].strip() not in options_for_none:
                    mapped_row[key] = row[option].strip()
    return mapped_row

def transform_row_info(row_info, target_mappings, join_key):
    '''transform the row info into a list of new row values'''
    new_rows = {}
    for row in row_info.values():
        if row['barcode'] == '1009-BS-05':

            transformed_row = transform_single_row_info(row, target_mappings)
            new_rows[transformed_row[join_key]] = transformed_row

    return list(new_rows.values())

def write_row_info(transformed_rows, column_headers, output_file_name):
    '''write the row info into the new csv to be processed by fhir_petl'''
    with open(output_file_name, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=column_headers)

        writer.writeheader()
        for row_dict in transformed_rows:
            writer.writerow(row_dict)

def generate_csv(in_files, file_join_key, mapping_join_key, output_file_name):
    '''generate csv using helper functions by mapping info to new mapped json object mappings'''
    row_info = generate_row_info(in_files.get('files'), file_join_key)

    transformed_row_info = transform_row_info(row_info, in_files.get('mappings'), mapping_join_key)
    write_row_info(transformed_row_info, in_files.get('column_headers'), output_file_name)



generate_csv(dm.ktb_patient_mapping, 'barcode', 'SID', 'Patients_ktb.csv')


# generate_csv(
#     dm.jackson_patient_mapping
#     , 'NEW UNIQUE CODE #'
#     , 'SID'
#     , 'Patients.csv'
# )


# generate_csv(
#     dm.medication2_mapping
#     , 'NEW UNIQUE CODE #'
#     , 'SID'
#     , 'Meds.csv'
# )


# generate_csv(
#     dm.condition_mapping
#     , 'NEW UNIQUE CODE #'
#     , 'SID'
#     , 'Conditions.csv'
# )
