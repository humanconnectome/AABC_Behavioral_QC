import os
import typing as T

import yaml
from memofn import load_cache, save_cache, memofn

load_cache(".cache_memofn")
import collections
import io
import re
import pandas as pd
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

from ccfbox import LifespanBox
from functions import (
    memo_get_frame,
    idvisits,
    filterdupass,
    parse_content,
    params_request_report,
    toolbox_to_dataframe,
    send_frame,
    get_aabc_arms_report,
    remove_test_subjects,
    cat_toolbox_score_files,
    cat_toolbox_rawdata_files,
    list_psychopy_subjects,
    qc_detect_test_subjects_in_production_database,
    qc_subjects_found_in_aabc_not_in_hca,
    qc_subject_id_is_not_missing,
    qc_subject_initiating_wrong_visit_sequence,
    qc_unable_to_locate_qint_data,
    qc_has_qint_but_id_visit_not_found_in_aabc,
    qc_duplicate_qint_records,
    qc_raw_or_scored_data_not_found,
    qc_toolbox_pins_not_in_aabc,
    qc_missing_toolbox_data,
    qc_unable_to_locate_asa24_id_in_redcap_or_box,
    qc_missing_actigraphy_data_in_box,
    qc_psychopy_not_found_in_neither_box_nor_intradb,
    qc_redcap_missing_counterbalance,
    qc_visit_summary_incomplete,
    is_v_event,
    qc_hot_flash_data,
    qc_vns_data,
    qc_bunk_ids_in_psychopy_and_actigraphy,
    qc_age_in_v_events,
    qc_bmi_in_v_events,
    register_tickets,
)
from config import LoadSettings

config = LoadSettings()
secret = pd.read_csv(config["config_files"]["secrets"])
api_key = secret.set_index("source")["api_key"].to_dict()
box = LifespanBox(cache="./tmp")
box.read_file_in_memory = memofn(box.read_file_in_memory, expire_in_days=1, ignore_first_n_args=0)


@memofn(expire_in_days=1)
def box_file_created_at(fileid):
    return box.get_metadata_by_id(fileid).created_at


box.list_of_files = memofn(box.list_of_files, expire_in_days=1, ignore_first_n_args=0)

## get the HCA inventory for ID checking with AABC
hca_inventory = box.read_csv(config["hcainventory"])


def list_files_in_box_folders(*box_folder_ids) -> pd.DataFrame:
    """List filename, fileid, sha1 for all files in specific box folders

    Args:
        *box_folder_ids: The box id for the folder of interest

    Returns:
        A dataframe with filename, fileid, sha1 for all files in the folder(s)

    """
    return pd.DataFrame(box.list_of_files(box_folder_ids).values())


#########################################################################################
# PHASE 0 TEST IDS AND ARMS
# if Legacy, id exists in HCA and other subject id related tests:
# Test that #visits in HCA corresponds with cohort in AABC
def get_aabc_inventory_from_redcap(redcap_api_token: str) -> pd.DataFrame:
    """Download the AABC inventory from RedCap.
    This does QC on the subject ids and returns only the cleaned up rows (excludes test subjects).

    Args:
        redcap_api_token: API token for the AABC redcap project

    Returns:
        A dataframe with the AABC inventory of participants
    """
    keeplist = [
        "actigraphy_collectyn",
        "age",
        "age_visit",
        "asa24id",
        "asa24yn",
        "bmi",
        "completion_mocayn",
        "counterbalance_1st",
        "counterbalance_2nd",
        "dob",
        "ethnic",
        "event_date",
        "face_complete",
        "height_ft",
        "height_in",
        "height_missing_jira",
        "height_outlier_jira",
        "legacy_yn",
        "nih_toolbox_collectyn",
        "nih_toolbox_upload_typo",
        "passedscreen",
        "psuedo_guid",
        "racial",
        "ravlt_collectyn",
        "redcap_event",
        "redcap_event_name",
        "sex",
        "site",
        "study_id",
        "subject",
        "subject_id",
        "tlbxwin_dups_v2",
        "v0_date",
        "visit_summary_complete",
        "vms_collectyn",
        "walkendur_dups",
        "weight",
        "PIN",
    ]
    aabc_inventory_including_test_subjects = get_aabc_arms_report(redcap_api_token)
    qc_detect_test_subjects_in_production_database(aabc_inventory_including_test_subjects)
    aabc_inventory = idvisits(aabc_inventory_including_test_subjects)
    aabc_inventory = remove_test_subjects(aabc_inventory, "subject_id")
    aabc_inventory = aabc_inventory[keeplist]
    return aabc_inventory


aabc_inventory = get_aabc_inventory_from_redcap(api_key["aabcarms"])
save_cache()

qc_subjects_found_in_aabc_not_in_hca(aabc_inventory, hca_inventory)
# TODO: On airflow, every hour
qc_subject_initiating_wrong_visit_sequence(aabc_inventory, hca_inventory)
qc_subject_id_is_not_missing(aabc_inventory)


def ravlt_form_heuristic(row):
    if "RAVLT-Alternate Form C" in row.content:
        form = "Form C"
    elif "RAVLT-Alternate Form D" in row.content:
        form = "Form D"
    elif "Form B" in row.filename:
        form = "Form B"
    else:
        raise ValueError("Form not found")
    return form


def cron_job_qint(qint_df: pd.DataFrame, qint_api_token) -> None:
    """
    This function is a cron job that scans the box folders for new files and imports them into Qinteractive. Because of
    the sheer number of requests it sends out, it can take a while to run. New files are defined as having a new fileid,
    new sha1, or new filename.

    Args:
        qint_df: Current Qinteractive dataframe
        qint_api_token: API token for Qinteractive
    """
    # the variables that make up the 'common' form in the Qinteractive database.
    common_form_fields = [
        "id",
        "redcap_data_access_group",
        "site",
        "subjectid",
        "fileid",
        "filename",
        "sha1",
        "created",
        "assessment",
        "visit",
        "form",
        "q_unusable",
        "unusable_specify",
        "common_complete",
        "ravlt_two",
    ]

    # the variables that make up the ravlt form
    ravlt_form_fields = [
        "ravlt_pea_ravlt_sd_tc",
        "ravlt_delay_scaled",
        "ravlt_delay_completion",
        "ravlt_discontinue",
        "ravlt_reverse",
        "ravlt_pea_ravlt_sd_trial_i_tc",
        "ravlt_pea_ravlt_sd_trial_ii_tc",
        "ravlt_pea_ravlt_sd_trial_iii_tc",
        "ravlt_pea_ravlt_sd_trial_iv_tc",
        "ravlt_pea_ravlt_sd_trial_v_tc",
        "ravlt_pea_ravlt_sd_listb_tc",
        "ravlt_pea_ravlt_sd_trial_vi_tc",
        "ravlt_recall_correct_trial1",
        "ravlt_recall_correct_trial2",
        "ravlt_recall_correct_trial3",
        "ravlt_recall_correct_trial4",
        "ravlt_delay_recall_correct",
        "ravlt_delay_recall_intrusion",
        "ravlt_delay_total_intrusion",
        "ravlt_delay_total_repetitions",
    ]

    # all box files - grab, transform, send
    folder_queue = ["WU", "UMN"]  # ,'MGH','UCLA']

    # Files that already exist in Q Redcap
    cached_filelist = qint_df.copy()
    cached_filelist.fileid = cached_filelist.fileid.astype("Int64")

    ds_vars = config["Redcap"]["datasources"]
    accumulator = []
    for site_accronym in folder_queue:
        box_folder_id = ds_vars["qint"]["BoxFolders"][site_accronym]
        data_access_group_name = ds_vars["aabcarms"][site_accronym]["dag"]
        site_number = ds_vars["aabcarms"][site_accronym]["sitenum"]

        box_filelist = list_files_in_box_folders(box_folder_id)
        box_filelist.fileid = box_filelist.fileid.astype(int)

        # find the new ones that need to be pulled in
        new_file_ids = set(box_filelist.fileid) - set(cached_filelist.fileid)
        new_files = box_filelist.loc[box_filelist.fileid.isin(new_file_ids)].copy()

        # Short-circuit #1: No new records to add
        if new_files.empty:
            print("NO NEW RECORDS from", site_accronym, "TO ADD AT THIS TIME")
            continue

        # initiate new ids
        next_id = cached_filelist.id.astype("Int64").max() + 1

        new_files["id"] = range(next_id, next_id + len(new_files))
        new_files["redcap_data_access_group"] = data_access_group_name
        new_files["site"] = site_number
        new_files["created"] = new_files.fileid.apply(box_file_created_at)
        new_files["content"] = new_files.fileid.apply(box.read_text)
        save_cache()
        new_files["form"] = new_files.apply(ravlt_form_heuristic, axis=1)
        new_files["assessment"] = "RAVLT"
        new_files["q_unusable"] = ""
        new_files["unusable_specify"] = ""
        new_files["common_complete"] = ""
        new_files["ravlt_two"] = ""

        id_visit = new_files.filename.str.extract("(?P<subjectid>HCA\d+)_V(?P<visit>\d)")

        ravlt = new_files.content.apply(lambda x: pd.Series(parse_content(x), index=ravlt_form_fields))

        # Horizontally concatenate the dataframes
        rows2push = pd.concat([new_files, ravlt, id_visit], axis=1)

        # Reorder the columns
        rows2push = rows2push[common_form_fields + ravlt_form_fields].copy()
        accumulator.append(rows2push)

    if accumulator:
        df = pd.concat(accumulator)
        send_frame(
            dataframe=df,
            tok=qint_api_token,
        )


# TODO: TODAY run on airflow every sunday at 6AM CT
# cron_job_qint(qint_df, qint_api_token)


def qint_code_block(aabc_inventory, qint_api_token):

    # FLOW:
    # Qinteractive  order:
    # # 1. grab new stuff from box
    # # 2. transform it
    # # 3. send it to REDCap
    # # 4. QC (incorporating patches)
    # # 5. generate tickets
    # # 6. send tickets that arent identical to ones already in Jira (now or at the end in a single bolus)
    # # 7. create and send snapshot of patched data to BOX after dropping restricted variables

    # Observed:
    # pull Q data from Box to qint REDCap, then query qint against AABC-Arms study ids and visit
    #    All records EVER created will be included in REDCap.
    #    duplications
    #    typos will be set to unusable automatically
    #    missing: look for potential records in REDCap, first.  Correct in REDCap Not BOX or it will lead to duplicate.
    #    if dup, set one to unususable and explain

    # current Qint Redcap:
    qint_report = params_request_report(
        token=qint_api_token,
        report_id="51037",
    )
    # QC checks
    # now check
    qint_df2 = memo_get_frame(api_url=config["Redcap"]["api_url10"], data=qint_report)
    qint_df2 = qint_df2.loc[qint_df2.q_unusable == "", ["id", "site", "subjectid", "visit"]].copy()
    qint_df2["redcap_event"] = "V" + qint_df2.visit
    qint_df2 = remove_test_subjects(qint_df2, "subjectid")
    qc_duplicate_qint_records(qint_df2)

    aabc_vs_qint = pd.merge(
        aabc_inventory,
        qint_df2.rename(columns={"subjectid": "subject"}).drop(columns=["site"]),
        on=["subject", "redcap_event"],
        how="outer",
        indicator=True,
    )
    aabc_vs_qint["has_qint_data"] = aabc_vs_qint._merge != "left_only"
    qc_has_qint_but_id_visit_not_found_in_aabc(aabc_vs_qint)

    aabc_inventory_plus_qint = aabc_vs_qint.loc[aabc_vs_qint._merge != "right_only"].drop(columns=["_merge"])
    qc_unable_to_locate_qint_data(aabc_inventory_plus_qint, aabc_vs_qint)

    return aabc_vs_qint, aabc_inventory_plus_qint


aabc_vs_qint, aabc_inventory_plus_qint = qint_code_block(aabc_inventory, api_key["qint"])


def generic_fetch_toolbox_data(specific_toolbox_fn: T.Callable, fix_typos_map: dict) -> pd.DataFrame:
    """Fetches the csv files using the specific_toolbox_fn and returns a dataframe with the data

    Args:
        specific_toolbox_fn: a function that takes site and returns a single string with all the csv data
        fix_typos_map: a dictionary with the keys being the typos and the values being the correct values

    Returns:
        A dataframe with the data from the csv files
    """

    def get_data(site):
        raw_text = specific_toolbox_fn(site)
        df = toolbox_to_dataframe(raw_text)
        return df

    df_wu = get_data("AABC_WU_ITK")
    df_mgh = get_data("AABC_MGH_ITK")
    df_umn = get_data("AABC_UMN_ITK")
    df_ucla = get_data("AABC_UCLA_ITK")
    save_cache()

    df = pd.concat([df_wu, df_umn, df_ucla, df_mgh])

    # remove files known to be duds.
    df = remove_test_subjects(df, "PIN")

    # fix typos
    df.PIN.replace(fix_typos_map, inplace=True)

    return df


def fetch_toolbox_raw_data(fix_typos_map: dict) -> pd.DataFrame:
    """Get DataFrame containing toolbox data from raw files for all sites

    Args:
        fix_typos_map: a dictionary with the keys being the typos and the values being the correct values

    Returns:
        dataframe of raw toolbox data
    """
    return generic_fetch_toolbox_data(cat_toolbox_rawdata_files, fix_typos_map)


def fetch_toolbox_score_data(fix_typos_map: dict) -> pd.DataFrame:
    """Get DataFrame containing toolbox data from score files for all sites

    Args:
        fix_typos_map: a dictionary with the keys being the typos and the values being the correct values

    Returns:
        dataframe of score toolbox data
    """
    return generic_fetch_toolbox_data(cat_toolbox_score_files, fix_typos_map)


def gen_fixtypos_map(aabc_vs_qint):
    fix_typos = aabc_vs_qint.loc[aabc_vs_qint.nih_toolbox_upload_typo != ""]
    correct_pin = fix_typos.subject + "_" + fix_typos.redcap_event
    fixtypos_map = dict(zip(fix_typos.nih_toolbox_upload_typo, correct_pin))
    return fixtypos_map


def toolbox_code_block(aabc_vs_qint, aabc_inventory_plus_qint):
    # NOW FOR TOOLBOX. ############################################################################
    # # 1. grab partial files from intraDB
    # # 2. QC (after incorporating patches)
    # # 3. generate tickets and send to JIra if don't already exist
    # # 4. send tickets that arent identical to ones already in Jira
    # # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
    # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

    fix_typos_map = gen_fixtypos_map(aabc_vs_qint)
    # This tb_raw_df is what is going to go to the snapshot, after dates are removed.
    tb_raw_df = fetch_toolbox_raw_data(fix_typos_map)

    # drop duplicates for purpose of generating QC flags.
    tb_raw_df = tb_raw_df.drop_duplicates(subset="PIN").copy()

    tbx_score_df = fetch_toolbox_score_data(fix_typos_map)

    # merge with patch fixes (i.e. delete duplicate specified in visit summary)
    # This is a single fix... need to generalized to all instruments and their corresponding dupvars:
    # -->HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"
    for instrument_name, _num, dups_field in config["toolbox_stringnumdupvar"]:
        tbx_score_df = filterdupass(tbx_score_df, aabc_vs_qint, dups_field, instrument_name)

    # drop identical rows
    tbx_score_df.drop_duplicates(inplace=True)
    tbx_score_df = tbx_score_df.merge(aabc_vs_qint, "left", "PIN")
    tb_raw_df = tb_raw_df.merge(aabc_vs_qint, "left", "PIN")
    # but find duplicates that match on "PIN" and "Inst" fields
    duplicate_assessments = tbx_score_df.loc[tbx_score_df.duplicated(subset=["PIN", "Inst"])]
    register_tickets(
        duplicate_assessments,
        "ORANGE",
        "Duplicate assessment in toolbox",
        # TODO: Add a code number here:
        "",
    )

    qc_raw_or_scored_data_not_found(tbx_score_df, tb_raw_df)

    # add subject and visit
    scores2 = tbx_score_df.PIN.drop_duplicates().str.extract("^(?P<PIN>(?P<subject>.+?)_(?P<redcap_event>.+))$")

    # now merge with inventory
    aabc_qint_tlbx = pd.merge(
        aabc_inventory_plus_qint,
        scores2,
        on=["subject", "redcap_event"],
        how="outer",
        indicator=True,
    )
    # filter out pins only in toolbox
    in_tlbx_only = aabc_qint_tlbx._merge == "right_only"
    qc_toolbox_pins_not_in_aabc(aabc_qint_tlbx.loc[in_tlbx_only])
    aabc_inventory_5 = aabc_qint_tlbx.loc[~in_tlbx_only]

    # create a field specifying whether the subject has toolbox scores data
    aabc_inventory_5["has_tlbx_data"] = aabc_inventory_5._merge == "both"
    aabc_inventory_5.drop(columns=["_merge"], inplace=True)

    qc_missing_toolbox_data(aabc_inventory_5)

    asa24_code_block(aabc_inventory_5)


def asa24_code_block(aabc_inventory_5):
    folder_queue = ["WU", "UMN", "MGH"]  # UCLA and MGH not started yet
    cache_file = "asa24_cached_data.yml"
    asa24_dict = load_dict_from_yaml(cache_file)

    for site_accronym in folder_queue:
        box_folder_id = config["NonQBox"]["ASA24"][site_accronym]
        files_dict = box.list_of_files([box_folder_id])
        save_cache()
        not_yet_visited = set(files_dict.keys()) - set(asa24_dict.keys())
        for fileid in not_yet_visited:
            k = box.read_csv(fileid)
            asa24_dict[fileid] = k.UserName.unique().tolist()

    save_dict_to_yaml(asa24_dict, cache_file)
    asa24ids = set(ID for id_list in asa24_dict.values() for ID in id_list)
    aabc_inventory_5["has_asa24_data"] = aabc_inventory_5.asa24id.isin(asa24ids)
    qc_unable_to_locate_asa24_id_in_redcap_or_box(aabc_inventory_5)
    actigraphy_code_block(aabc_inventory_5)


def load_dict_from_yaml(cache_filename: str) -> T.Dict[str, T.Any]:
    """Load from YAML file the dictionary of `values` with Box file ids as keys.

    Args:
        cache_filename: The YAML file to load the values from.

    Returns:
        values: The dictionary of values
    """
    if not os.path.isfile(cache_filename):
        return dict()
    else:
        with open(cache_filename, "r") as f:
            values = yaml.safe_load(f)
        return values


def save_dict_to_yaml(values: T.Dict[str, T.Any], cache_filename: str) -> None:
    """Save the dictionary of values to a yaml file.

    Args:
        values: The dictionary of values to save.
        cache_filename: The YAML file to save the values to.
    """

    with open(cache_filename, "w") as f:
        yaml.dump(values, f)


def actigraphy_code_block(aabc_inventory_6):
    cache_file = "actigraphy_cached_data.yml"
    actigraphy_dict = load_dict_from_yaml(cache_file)

    folder_queue = ["WU", "UMN", "MGH"]  # ,'UCLA']
    for site_accronym in folder_queue:
        print(site_accronym)
        box_folder_id = config["NonQBox"]["Actigraphy"][site_accronym]
        files_dict = box.list_of_files([box_folder_id])
        save_cache()
        not_yet_visited = set(files_dict.keys()) - set(actigraphy_dict.keys())
        for fileid in not_yet_visited:
            text_content = box.read_text(fileid)
            match_result = re.search('"(HCA[^"]+)"', text_content)
            if not match_result:
                print("Something is wrong with this file: ", fileid)
                continue
            hcaid = match_result.groups()[0]
            actigraphy_dict[fileid] = hcaid

    save_cache()
    save_dict_to_yaml(actigraphy_dict, cache_file)
    actigraphy_PINs = list(actigraphy_dict.values())
    # Duplicates?
    act_counter = collections.Counter(actigraphy_PINs)
    duplicated_actigraphy_records = [pin for pin, count in act_counter.items() if count > 1]
    register_tickets(
        aabc_inventory_6.loc[aabc_inventory_6.PIN.isin(duplicated_actigraphy_records)],
        # TODO: add code? "ORANGE"? etc.
        "",
        "Duplicate actigraphy records Found",
        # TODO: Add a code number here:
        "",
        coordinator_only=True,
    )
    aabc_inventory_6["has_actigraphy_data"] = aabc_inventory_6.PIN.isin(actigraphy_PINs)
    qc_missing_actigraphy_data_in_box(aabc_inventory_6)
    psychopy_code_block(aabc_inventory_6)


def psychopy_code_block(inventoryaabc6):

    ############################################################################
    # Psychopy
    # ORDER
    # 1. scan for data in Box (here just looking for existence) and in IntraDB
    # 2. QC ids (after incorporating patches)
    # 3. generate an tickets and send to JIra if don't already exist
    # 4. DONT dump or snapshot.  Leave data in IntraDB.

    site_accronym = "WU"
    folder_queue = ["WU", "MGH", "UMN"]  # UCLA

    # TODO: cache results to yaml

    # scan Box
    psychopy_files_list = []
    for site_accronym in folder_queue:
        box_folder_id = config["NonQBox"]["Psychopy"][site_accronym]
        intradb_df = list_files_in_box_folders(box_folder_id)
        save_cache()
        fn = intradb_df.filename
        parts_of_fn = fn.str.extract("(?P<PIN_AB>(?P<subject>HCA\d+)_(?P<redcap_event>V\d)_(?P<scan>[A-Z]))")

        # 4 fields = filename, PIN_AB, subject, redcap_event, scan
        df = pd.concat([fn, parts_of_fn], axis=1)
        psychopy_files_list.append(df)
    psychopy_df = pd.concat(psychopy_files_list)

    # TODO: Ask if files that don't have `PIN_AB` available should generate tickets? Example includes practice runs like 'CARIT_HCA8860898_V3_run0_2022-09-21_111428_design.csv'
    bad_file_names = psychopy_df.loc[psychopy_df.PIN_AB.isna(), "filename"]
    # proceed with good filenames
    psychopy_df = psychopy_df[psychopy_df.PIN_AB.notna()].drop_duplicates().copy()

    # just check for existence of PsychoPY in IntraDB
    psychopy_subjects_on_intradb = "\n".join(
        [
            list_psychopy_subjects("AABC_WU_ITK").strip(),
            list_psychopy_subjects("AABC_UMN_ITK").strip(),
            list_psychopy_subjects("AABC_UCLA_ITK").strip(),
            list_psychopy_subjects("AABC_MGH_ITK").strip(),
        ]
    )
    # remove test suffix
    psychopy_subjects_on_intradb = psychopy_subjects_on_intradb.replace("t", "")

    in_intradb = set(psychopy_subjects_on_intradb.splitlines()) - set("")
    in_box = set(psychopy_df.PIN_AB)

    # TODO: Ask if it is okay to drop the scan, for merging purposes with `inventoryaabc6`? By losing scan, in_intradb goes from 88->47, in_box 108->57 due to _A/_B merging into a single value
    in_intradb = set(x.rsplit("_", 1)[0] for x in in_intradb)
    in_box = set(x.rsplit("_", 1)[0] for x in in_box)

    inventoryaabc6["PIN"] = inventoryaabc6.subject + "_" + inventoryaabc6.redcap_event

    register_tickets(
        inventoryaabc6[inventoryaabc6.PIN.isin(in_box - in_intradb)],
        "ORANGE",
        "PsychoPy data is in Box but not in IntraDB",
        # TODO: Add an error_code
        "",
    )

    register_tickets(
        inventoryaabc6[inventoryaabc6.PIN.isin(in_intradb - in_box)],
        "ORANGE",
        "PsychoPy data is in IntraDB but not in Box",
        # TODO: Add an error_code
        "",
    )

    inventoryaabc6["has_psychopy_data"] = ~inventoryaabc6.PIN.isin(in_box | in_intradb)
    aabc_visits = inventoryaabc6.loc[is_v_event(inventoryaabc6)].copy()
    qc_psychopy_not_found_in_neither_box_nor_intradb(aabc_visits)
    qc_redcap_missing_counterbalance(inventoryaabc6)
    qc_hot_flash_data()
    qc_vns_data()
    qc_bunk_ids_in_psychopy_and_actigraphy()
    qc_visit_summary_incomplete(aabc_visits)
    qc_age_in_v_events(aabc_visits)
    qc_bmi_in_v_events(aabc_visits)


toolbox_code_block(aabc_vs_qint, aabc_inventory_plus_qint)

# TO DO
# HARMONIZE Event Names
# Add filenames to TLBX data
# SEND INVENTORY, REDCAPs, and TLBX to PreRelease BOX
# HCA drop confusing variables
# inventoryaabc5.to_csv('Inventory_Beta.csv',index=False)
##upload

#    Check IntraDB for file and IDs therein - read and apply patch
# Actigraphy
#    Check IntraDB for file and IDs therein - read and apply patch
# VNS
#   Check IntraDB for file and IDs therein - read and apply patch
# ASR Spanish
#    Searcj for spanish language ASR, and upload subset of questions to REDCap.
#    Check that this has been done
# Moca
#    Look for missing MOCA, check for file, and ping RA to upload.
