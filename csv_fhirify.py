"""
CSV FHIRIFY
Author: Kevin Wood
Purpose: Transform flat file into fhir petl-ready csv
"""
import csv
import sys
import copy
import data_mappings as dm
import pandas as pd
import time
from fhir_petl.util import resolve


def get_current_row(row_info, join_key_value):
    """check if key is in the csv"""
    if join_key_value in row_info:
        return row_info[join_key_value]
    return {}


def get_row_indices(all_headers, to_keep):
    """get the indices of each row"""
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
    """generate information about the row"""
    row_info = {}
    first_line_metadata = "first_line_metadata"
    header_meta_dict = dict()
    for file_name in in_files.keys():
        with open(resolve(file_name), mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            headers = next(reader, None)
            headers = [x.strip(" ") for x in headers]

            if in_files[file_name]["skip_first_line"]:
                next(reader, None)

            if in_files[file_name][first_line_metadata]:
                header_meta_desc = file.readline()
                header_meta_desc = [
                    meta_header for meta_header in header_meta_desc.split(",")
                ]

                header_meta_dict = dict(zip(headers, header_meta_desc))

            if "columns_to_keep" in in_files[file_name]:
                headers_to_keep = in_files[file_name]["columns_to_keep"]
            else:
                headers_to_keep = in_files[file_name]["multiple_columns_to_keep"]

            row_indices = get_row_indices(headers, headers_to_keep)
            for row in reader:
                join_key_col_index = headers.index(join_key)
                current_row_info = get_current_row(row_info, row[join_key_col_index])

                for index in row_indices:
                    current_row_info[headers[index]] = row[index]

                row_info[row[join_key_col_index]] = current_row_info

    return row_info, header_meta_dict


def transform_single_row_info(row, target_mappings):
    """transform the row info for a single row into a row mapped to target keys"""
    mapped_row = {}
    options_for_none = ["None", "NA", "N/A", "?", "#VALUE!"]
    for key in target_mappings.keys():
        mapping_options = target_mappings[key]
        if len(mapping_options) == 1 and mapping_options[0] in row:
            mapped_row[key] = row[mapping_options[0]].strip()
        elif len(mapping_options) > 1:
            # iterate through mapping_options in reverse because to give the first element in
            # target_mappings priority over other elements in mapping list
            for option in reversed(mapping_options):
                if option in row and row[option].strip() not in options_for_none:
                    mapped_row[key] = row[option].strip()
    # returns single dictionary with key-value pairs
    return mapped_row


def parse_delimited_multi_value(
    field_value, field_delimiter, secondary_field_delimiter=None
):
    value_list = field_value.split(field_delimiter)
    if secondary_field_delimiter:
        value_list = double_parse_multi_value(value_list, secondary_field_delimiter)

    return value_list


def double_parse_multi_value(code_value_list, code_value_delimiter):
    code_value_list_final = []
    for code_value in code_value_list:
        temp_cv_list = code_value.split(code_value_delimiter)
        code_value_list = [cv.strip(" ") for cv in temp_cv_list]
        code_value_list_final.append(code_value_list)

    return code_value_list_final


def transform_transpose_row_info(
    row_in, target_mappings_in, transpose_mappings_in, header_meta_dict_in=dict(),
):
    options_for_none = ["None", "NA", "N/A", "?", "#VALUE!", "-9", ""]
    mapped_row_list = []
    for transpose_mapping_dict in transpose_mappings_in:
        transpose_mapping_name = transpose_mapping_dict["mapping_name"]
        if (
            transpose_mapping_name in row_in
            and row_in[transpose_mapping_name].strip() not in options_for_none
        ):

            parsed_values_list = parse_delimited_multi_value(
                row_in[transpose_mapping_name],
                transpose_mapping_dict["mapping_delimiter"],
                transpose_mapping_dict.get("secondary_mapping_delimiter", None),
            )
            for value in parsed_values_list:
                single_row_dict = transform_single_row_info(row_in, target_mappings_in)
                for target_mapping in target_mappings_in:
                    if transpose_mapping_name in target_mappings_in[target_mapping]:
                        if isinstance(value, list):
                            single_row_dict[target_mapping] = value[0]
                        else:
                            single_row_dict[target_mapping] = value

                    elif (
                        not target_mappings_in[target_mapping]
                        and isinstance(value, list)
                        and len(value) > 0
                    ):
                        single_row_dict[target_mapping] = value[-1]

                    mapped_row_list.append(single_row_dict)
    return mapped_row_list


def transform_multi_row_info(
    row_in,
    target_mappings_in,
    value_mappings_in,
    value_type_mappings_in,
    date_mappings_in,
    transpose_mappings_in,
    header_meta_dict_in=dict(),
):
    """transform the row info for a single row into muliple rows mapped to target keys"""
    mapped_row_list = []
    options_for_none = ["None", "NA", "N/A", "?", "#VALUE!", ""]

    if transpose_mappings_in and len(transpose_mappings_in) > 0:
        mapped_row_list = transform_transpose_row_info(
            row_in, target_mappings_in, transpose_mappings_in, header_meta_dict_in
        )
    else:
        for value_mapping_option, value_type_mapping_option, date_mapping_option in zip(
            value_mappings_in, value_type_mappings_in, date_mappings_in,
        ):
            if (
                value_mapping_option in row_in
                and row_in[value_mapping_option].strip() not in options_for_none
            ):

                single_row_dict = transform_single_row_info(row_in, target_mappings_in)
                single_row_dict["VALUE"] = row_in[value_mapping_option].strip()
                single_row_dict["VALUE_CODE"] = value_mapping_option
                if (
                    value_mapping_option in header_meta_dict_in.keys()
                    and header_meta_dict_in
                ):
                    single_row_dict["CODE_DESC"] = header_meta_dict_in[
                        value_mapping_option
                    ]

                single_row_dict["VALUE_TYPE"] = value_type_mapping_option
                if (
                    date_mapping_option in row_in
                    and row_in[date_mapping_option].strip() not in options_for_none
                ):
                    single_row_dict["VALUE_DATE"] = row_in[date_mapping_option]
                mapped_row_list.append(single_row_dict)

        # sys.exit()
        # returns single list with multiple dictionaries of SID, VALUE, and OBSERVATION_DATE key-value pairs
        # return [
        #     {"SID": "2956", "VALUE": "21", "OBSERVATION_DATE": "2008"},
        #     {"SID": "2956", "VALUE": "24", "OBSERVATION_DATE": "2008"},
        # ]
    return mapped_row_list


# return a list
def transform_single_row_to_multi_row(
    row_info,
    target_mappings=[],
    value_mappings=[],
    value_type_mappings=[],
    date_mappings=[],
    transpose_mappings=[],
    header_meta_dict_in=dict(),
):
    new_multi_rows_list = []
    # row is dictionary for each barcode's column header (key) and cell value (value)
    for row in row_info.values():
        transformed_multi_row = transform_multi_row_info(
            row,
            target_mappings,
            value_mappings,
            value_type_mappings,
            date_mappings,
            transpose_mappings,
            header_meta_dict_in=header_meta_dict_in,
        )
        new_multi_rows_list += transformed_multi_row
    return new_multi_rows_list


# return a list
def transform_single_row_to_single_row(row_info, target_mappings, join_key):
    """transform the row info into a list of new row values"""
    new_rows = {}
    for row in row_info.values():
        # raw row {'barcode': 'E100650', 'agediag': '40', 'agefrsbr': '21', 'donation year': '2018', 'subject_id': '2953'}
        # transformed row:  {'SID': '2953', 'VALUE': '40', 'OBSERVATION_DATE': '2018'}
        ## BUG: Raw row has all necessary information, but transformed row maps to the data_mappings
        # which defaults to the first of the two mapping rows specified in the data_mappings.py structure
        # Need to allow for multiple line items being created first
        # then allow for the column code and display values to be listed as their own fields, even though
        # it will be redundant.
        # if condition for multi-row transformation met:
        # transformed_row = transform_multi_row_info(row, target_mappings)
        # {value: 40, sid: 2468}
        # [
        #     {value: 40, sid: 2468}
        #     ,{value: 40, sid: 2468}
        # ]
        # add list ^ to our overall list
        # return list
        # else:
        transformed_row = transform_single_row_info(row, target_mappings)
        # add transformed row to new_rows
        new_rows[transformed_row[join_key]] = transformed_row
    return list(new_rows.values())


def write_row_info(transformed_rows, column_headers, output_file_name):
    """write the row info into the new csv to be processed by fhir_petl"""
    options_for_none = ["None", "NA", "N/A", "?", "#VALUE!", "-9", ""]
    with open(output_file_name, mode="w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=column_headers)

        writer.writeheader()
        for row_dict in transformed_rows:
            writer.writerow(row_dict)


def find_duplicates(row_info):
    current_position = 0
    key_list = list(row_info.keys())
    match_list = []
    for key in key_list:
        found_position = 0
        for found_key in key_list:
            if (
                current_position != found_position
                and key[1:] == found_key[1:]
                and sorted([key, found_key]) not in match_list
            ):
                match_list.append(sorted([key, found_key]))
            found_position += 1
        current_position += 1
    return match_list


def remove_duplicate_row_info(duplicate_values_in, row_info_in):
    row_info_deep_copy = copy.deepcopy(row_info_in)
    for pop_list in duplicate_values_in:
        for SID in pop_list:
            row_info_deep_copy.pop(SID, None)
    return row_info_deep_copy


def merge_dict_overwrite_first(dict1, dict2):
    """Desired result is a new dictionary with the values merged,
    and the second dict's values overwriting those from the first in pythonic syntax."""
    return {**dict1, **dict2}


def clean_duplicates_from_row_info(
    row_info_in, file_join_key_in, priority_key_criteria_in
):
    """parent function to find and merge duplicates while deleting all leftover duplicates based on SID"""
    duplicate_values = find_duplicates(row_info_in)
    merged_duplicates = merge_duplicates(
        row_info_in, duplicate_values, file_join_key_in, priority_key_criteria_in
    )

    dupes_removed_row_info = remove_duplicate_row_info(duplicate_values, row_info_in)
    de_duped_clean_row_info = merge_dict_overwrite_first(
        dupes_removed_row_info, merged_duplicates
    )

    return de_duped_clean_row_info


def merge_to_master_dict(
    duplicated_row_info_in, file_join_key_in, priority_key_criteria_in
):
    master_dict_out = {}

    for key, value in duplicated_row_info_in.items():
        if isinstance(value, list):
            for sub_val in value:
                if (
                    sub_val
                    and file_join_key_in == key
                    and priority_key_criteria_in in sub_val
                ):
                    master_dict_out[key] = sub_val

    return master_dict_out


def choose_value_by_key(value1, value2, override_with_value1):
    if value1 and value2:
        if override_with_value1:
            return value1
        else:
            return value2
    else:
        if value1:
            return value1
        elif value2:
            return value2
        else:
            return ""


def determine_override_key(
    dict1_in, dict2_in, file_join_key_in, priority_key_criteria_in
):
    """given the info about the barcode, then assign the boolean to which value to override"""
    if (
        file_join_key_in in dict1_in
        and dict1_in[file_join_key_in][0] == priority_key_criteria_in
    ):
        return True
    elif dict2_in[file_join_key_in][0] == priority_key_criteria_in:
        return False


def merge_dict(dict1, dict2, file_join_key_in, priority_key_criteria_in):
    """ Merge dictionaries and keep values of common keys in list"""
    dict3 = {}
    override_with_value1 = determine_override_key(
        dict1, dict2, file_join_key_in, priority_key_criteria_in
    )
    for key, value in dict1.items():
        if key in dict1 and key in dict2:
            # pass which of the two values is tied to the priority_key_criteria_in
            dict3[key] = choose_value_by_key(value, dict2[key], override_with_value1)
    return dict3


def merge_duplicates(
    row_info_in, match_list_in, file_join_key_in, priority_key_criteria_in
):
    de_duped_row_info = {}
    # duplicate_match_row_info = {}
    for nested_match_list in match_list_in:
        match_1_dict = row_info_in[nested_match_list[0]]
        match_2_dict = row_info_in[nested_match_list[1]]
        duplicate_match_row_info = merge_dict(
            match_1_dict, match_2_dict, file_join_key_in, priority_key_criteria_in
        )
        # for nested_match in nested_match_list:
        #     combined_match_row_info = merge_dict(duplicate_match_row_info, row_info_in[nested_match], file_join_key_in, priority_key_criteria_in)
        # master_dict = merge_to_master_dict(duplicate_match_row_info, file_join_key_in, priority_key_criteria_in)
        de_duped_row_info[
            duplicate_match_row_info[file_join_key_in]
        ] = duplicate_match_row_info

    return de_duped_row_info


def gen_dict_extract(key, var):
    if hasattr(var, "iteritems"):
        for k, v in var.iteritems():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result


def generate_csv(
    in_files,
    file_join_key_in,
    mapping_join_key,
    output_file_name,
    priority_key_criteria_in="",
    merge_duplicate_record_bool=True,
):
    """generate csv using helper functions by mapping info to new mapped json object mappings"""
    # row_info is a dictionary of dictionaries with SID as key and value as dictionary of column headers as keys and cell values as values
    start_time = time.time()
    row_info, header_meta_desc_dict = generate_row_info(
        in_files.get("files"), file_join_key_in
    )
    if merge_duplicate_record_bool:

        row_info = clean_duplicates_from_row_info(
            row_info, file_join_key_in, priority_key_criteria_in
        )

    # one is returning a list (existing), the other should also return a list
    # before, every time we get a new row, transform the row from that
    transformed_row_info = []
    if in_files["multi_resource_per_row_bool"]:

        transformed_row_info = transform_single_row_to_multi_row(
            row_info,
            target_mappings=in_files.get("target_mappings"),
            value_mappings=in_files.get("value_mappings"),
            value_type_mappings=in_files.get("value_type_mappings"),
            date_mappings=in_files.get("date_mappings"),
            transpose_mappings=in_files.get("transpose_mappings"),
            header_meta_dict_in=header_meta_desc_dict,
        )
    else:
        transformed_row_info = transform_single_row_to_single_row(
            clean_master_row_info, in_files.get("mappings"), mapping_join_key
        )
    if in_files["subject_metadata_file"]:
        subject_metadata = pd.read_csv((in_files["subject_metadata_file"]))
        transformed_row_info = join_data(
            transformed_row_info,
            "SID",
            subject_metadata,
            "person_id",
            final_column_list=in_files.get("column_headers"),
        )

    print("--- %s seconds ---" % (time.time() - start_time))
    write_row_info(
        transformed_row_info, in_files.get("column_headers"), output_file_name
    )


def join_data(data1, join_key1, data2, join_key2, final_column_list=[]):
    if not isinstance(data1, pd.DataFrame):
        data1 = pd.DataFrame(data1)
    if not isinstance(data2, pd.DataFrame):
        data2 = pd.DataFrame(data2)
    merged_df = data1.merge(data2, left_on=join_key1, right_on=join_key2)
    merged_df.drop_duplicates(inplace=True)
    final_df = merged_df[final_column_list].copy()
    return final_df.to_dict("records")


# generate_csv(
#     dm.patient_mapping_1,
#     "barcode",
#     "SID",
#     "Patients_.csv",
#     priority_key_criteria_in="K",
# )

# generate_csv(
#     dm.medication_mapping_test,
#     "barcode",
#     "SID",
#     "Medication_test.csv",
#     priority_key_criteria_in="K",
# )

# generate_csv(
#     dm.medication_mapping_ktb,
#     "barcode",
#     "SID",
#     "Medication_test123.csv",
#     priority_key_criteria_in="K",
# )

# generate_csv(dm.condition_mapping_test, "barcode", "SID", "Condition_test.csv", priority_key_criteria_in="K")


# generate_csv(dm.observation_mapping_test, "barcode", "SID", "Observation_test.csv", priority_key_criteria_in="K")

# generate_csv(
#     dm.observation_age_at_donation_ktb,
#     "barcode",
#     "SID",
#     "Observation_age_at_donation.csv",
#     priority_key_criteria_in="K",
#     merge_duplicate_record_bool=False,
# )

# generate_csv(
#     dm.observation_mapping_ktb,
#     "barcode",
#     "SID",
#     "Observation_ktb2.csv",
#     priority_key_criteria_in="K",
# )
# generate_csv(
#     dm.condition_mapping_ktb,
#     "barcode",
#     "SID",
#     "Condition_ktb.csv",
#     priority_key_criteria_in="K",
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
