import csv
import pandas as pd
import difflib


def ref_file_name_lookup(
    ref_file_name="",
    ref_join_col="",
    ref_rename_col="",
    csv_fhir_file_name="",
    csv_fhir_join_col="",
    csv_fhir_rename_col="",
    final_column_list=list(),
    ref_rename_col_backup=None,
):
    # parameters: ref_file_name w/ path, join column ref_file, csv-fhir csv file_name w/ path, join column csv-fhir, final columns list
    # read ref csv as df
    ref_df = pd.read_csv(ref_file_name)
    print("\n ref_df unique: \n", ref_df[ref_join_col].unique())
    print("\n ref_df data types: \n", ref_df.dtypes)
    print("\n ref_df \n", ref_df.head())
    # read csv-fhir csv as df
    csv_fhir_df = pd.read_csv(csv_fhir_file_name)
    print("\n csv_fhir_df unique: \n", csv_fhir_df[csv_fhir_join_col].unique())
    print("\n csv_fhir_df data types: \n", csv_fhir_df.dtypes)
    print("\n csv_fhir_df \n", csv_fhir_df.head())

    # Staging
    # join on join column to create new df

    stage_df = csv_fhir_df.merge(
        ref_df, how="inner", left_on=csv_fhir_join_col, right_on=ref_join_col
    )
    print("\n stage_df \n", stage_df.head())
    print("\n stage_df after initial join with ref count \n", stage_df.count())

    # remove unnecessary columns and fillna with ""
    print("\n stage_df before combine_firsts count \n", stage_df.count())
    print(stage_df.sample(n=10))
    stage_df[csv_fhir_rename_col] = stage_df[csv_fhir_rename_col].combine_first(
        stage_df[ref_rename_col_backup]
    )
    stage_df[csv_fhir_rename_col] = stage_df[csv_fhir_rename_col].combine_first(
        stage_df[ref_rename_col]
    )
    print(stage_df.sample(n=10))
    print("\n stage_df after combine_firsts count \n", stage_df.count())

    idx = (stage_df[csv_fhir_join_col] == 199) & (
        stage_df[csv_fhir_rename_col] != "Vitamins/Herbal medicine"
    )

    # set Other columns to new code
    idx2 = (stage_df[csv_fhir_join_col] == 200) & (
        stage_df[csv_fhir_rename_col] != "Other"
    )
    print("\n stage_df before idx count \n", stage_df.count())
    stage_df.loc[idx, [csv_fhir_join_col]] = stage_df.loc[
        idx, [csv_fhir_rename_col]
    ].values
    print("\n stage_df after idx count \n", stage_df.count())

    stage_df.loc[idx2, [csv_fhir_join_col]] = stage_df.loc[
        idx2, [csv_fhir_rename_col]
    ].values
    print("\n stage_df after idx2 count \n", stage_df.count())

    # if code populated, but name is None, look up name in ref df to fill name
    code_name_idx = (
        (stage_df[csv_fhir_join_col] != "None") | (stage_df[csv_fhir_join_col] != "")
    ) & (
        (stage_df[csv_fhir_rename_col] == "None")
        | (stage_df[csv_fhir_rename_col] == "")
    )
    print("\n stage_df before .loc count \n", stage_df.count())
    stage_df.loc[code_name_idx, [csv_fhir_rename_col]] = stage_df.loc[
        code_name_idx, [ref_rename_col_backup]
    ].values

    # fuzzy join merge
    stage_df[csv_fhir_rename_col] = stage_df[csv_fhir_rename_col].astype(str)
    ref_df[ref_rename_col_backup] = ref_df[ref_rename_col_backup].astype(str)
    print("\n stage_df after .loc count \n", stage_df.count())
    ref_df[ref_rename_col_backup] = ref_df[ref_rename_col_backup].apply(
        lambda x: difflib.get_close_matches(x, stage_df[csv_fhir_rename_col])[0]
    )

    fuzzy_stage_df = stage_df.merge(ref_df).copy()
    print("\n fuzzy_stage_df \n", fuzzy_stage_df.head())
    print("\n fuzzy_stage_df after fuzzy ref merge count \n", fuzzy_stage_df.count())

    # Prep final dataframe
    final_df = fuzzy_stage_df[final_column_list].copy()
    final_df.fillna("Unknown", inplace=True)
    final_df = final_df[final_df[csv_fhir_join_col] != "None"].copy()
    final_df[csv_fhir_join_col] = final_df[csv_fhir_join_col].astype(str)
    final_df[csv_fhir_join_col] = (
        final_df[csv_fhir_join_col].str.strip().str.upper().str.replace(" ", "_")
    )
    print("\n final_df count \n", final_df.count())
    final_df.sort_values(by=[final_column_list[0]], inplace=True)
    print("\n final_df count \n", final_df.count())
    final_df.drop_duplicates(keep="first", inplace=True)
    # print("\n final_df count \n", final_df.count())

    print("\n final_df \n", final_df.head())

    # write final dataframe to csv
    return final_df.to_csv("Updated_" + csv_fhir_file_name, index=False)


# def subject_lookup(subject_file, subject_join, main_df, main_join):


ref_file_name_lookup(
    ref_file_name="ktb_drug_list.csv",
    ref_join_col="Code",
    ref_rename_col="(Generic name)*",
    csv_fhir_file_name="Medication_test123.csv",
    csv_fhir_join_col="DRUG_CODE",
    csv_fhir_rename_col="DRUG_NAME",
    final_column_list=[
        "SID",
        "DRUG_NAME",
        "DRUG_CODE",
        # "CODE_DESC",
        "START_DATE",
        "subject",
    ],
    ref_rename_col_backup="Brand name",
)
