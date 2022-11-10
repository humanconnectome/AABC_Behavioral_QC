import os
import typing

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
    qc_psychopy_not_found_in_box_or_intradb,
    qc_redcap_missing_counterbalance,
    qc_visit_summary_incomplete,
    is_v_event,
    qc_hot_flash_data,
    qc_vns_data,
    qc_bunk_ids_in_psychopy_and_actigraphy,
    qc_age_in_v_events,
    qc_bmi_in_v_events,
    cron_clean_up_aabc_inventory_for_recruitment_stats,
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
    aabc_inventory_including_test_subjects = get_aabc_arms_report(redcap_api_token)
    qc_detect_test_subjects_in_production_database(aabc_inventory_including_test_subjects)
    aabc_inventory = idvisits(aabc_inventory_including_test_subjects)
    aabc_inventory = remove_test_subjects(aabc_inventory, "subject_id")
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
    keeplist = [
        "study_id",
        "redcap_event_name",
        "v0_date",
        "dob",
        "age",
        "sex",
        "legacy_yn",
        "psuedo_guid",
        "ethnic",
        "racial",
        "site",
        "passedscreen",
        "subject_id",
        "subject",
        "redcap_event",
        "counterbalance_1st",
        "counterbalance_2nd",
        "height_ft",
        "height_in",
        "weight",
        "bmi",
        "height_outlier_jira",
        "height_missing_jira",
        "age_visit",
        "event_date",
        "completion_mocayn",
        "ravlt_collectyn",
        "nih_toolbox_collectyn",
        "nih_toolbox_upload_typo",
        "tlbxwin_dups_v2",
        "actigraphy_collectyn",
        "vms_collectyn",
        "face_complete",
        "visit_summary_complete",
        "asa24yn",
        "asa24id",
    ]

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
    qint_df2 = qint_df2[["id", "site", "subjectid", "visit"]].copy()
    qint_df2["redcap_event"] = "V" + qint_df2.visit
    qint_df2 = remove_test_subjects(qint_df2, "subjectid")
    # Before merging, check for duplicates that haven't been given the 'unusable' flag
    qc_duplicate_qint_records(qint_df2)

    aabc_vs_qint = pd.merge(
        aabc_inventory[keeplist],
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


def generic_fetch_toolbox_data(specific_toolbox_fn: typing.Callable, fix_typos_map: dict) -> pd.DataFrame:
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
    tbx_score_df = filterdupass(
        tbx_score_df,
        dups_df=aabc_vs_qint,
        dups_field="tlbxwin_dups_v2",
        instrument_name="NIH Toolbox Words-In-Noise Test Age 6+ v2.1",
    )

    # drop identical rows
    tbx_score_df.drop_duplicates(inplace=True)
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
    ### NOW For ASA 24 ######################################################################
    # ORDER
    # 1. scan for data (here just looking for existende)
    # 2. QC ids (after incorporating patches and translating ASAID into AABC id)
    # 3. generate tickets and send to JIra if don't already exist
    #  4. send tickets that arent identical to ones already in Jira
    # # # 5. just dump all legit data to BOX (transform to be defined later) after patching, dropping restricted variables, and merging in subject and redcap_event
    # # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

    folder_queue = ["WU", "UMN", "MGH"]  # UCLA and MGH not started yet
    already_visited, asa24ids = load_variables_from_yaml()

    for site_accronym in folder_queue:
        box_folder_id = config["NonQBox"]["ASA24"][site_accronym]
        files_df = list_files_in_box_folders(box_folder_id)
        save_cache()
        current = set(files_df.itertuples(index=False, name=None))
        new = current - already_visited
        for filename, fileid, sha1 in new:
            k = box.read_csv(fileid)
            asa24ids.update(k.UserName)
            already_visited.add((filename, fileid, sha1))

    save_variables_to_yaml(already_visited, asa24ids)

    AD = pd.DataFrame(asa24ids, columns=["asa24id"])
    aabc_inventory_6 = pd.merge(aabc_inventory_5, AD, on="asa24id", how="left", indicator=True)
    aabc_inventory_6["has_asa24_data"] = aabc_inventory_6._merge != "left_only"
    qc_unable_to_locate_asa24_id_in_redcap_or_box(aabc_inventory_6)
    aabc_inventory_6.drop(columns=["_merge"], inplace=True)
    actigraphy_code_block(aabc_inventory_6)


def save_variables_to_yaml(already_visited, asa24ids):
    with open("asa24_cached_data.yml", "w") as f:
        already_visited = sorted(list(x) for x in already_visited)
        yaml.dump({"asa24ids": sorted(asa24ids), "already_visited": already_visited}, f)


def load_variables_from_yaml():
    if not os.path.isfile("asa24_cached_data.yml"):
        asa24ids = set()
        already_visited = set()
    else:
        with open("asa24_cached_data.yml", "r") as f:
            y = yaml.safe_load(f)
        asa24ids = set(y["asa24ids"])
        already_visited = set(tuple(row) for row in y["already_visited"])
    return already_visited, asa24ids


def actigraphy_code_block(aabc_inventory_6):
    #################################################################################
    # ACTIGRAPHY
    ### for now, this is basically the same protocol as for ASA24

    # scan BOX
    folder_queue = ["WU", "UMN", "MGH"]  # ,'UCLA']
    actdata = []
    for site_accronym in folder_queue:
        print(site_accronym)
        box_folder_id = config["NonQBox"]["Actigraphy"][site_accronym]
        dbitems = list_files_in_box_folders(box_folder_id)
        for fid in dbitems.fileid:
            try:
                file_one: io.BytesIO = box.read_file_in_memory(fid)
                text_content = file_one.decode()
                hcaid = re.search('"(HCA[^"]+)"', text_content).groups()[0]
                actdata.append(hcaid)
            except:
                print("Something the matter with file", fid)

    save_cache()
    # Duplicates?
    duplicated_actigraphy_records = [item for item, count in collections.Counter(actdata).items() if count > 1]
    # TODO: create tickets that get sent to Petra only (by omitting site)
    if len(duplicated_actigraphy_records) != 0:
        print(
            "Duplicated Actigraphy Record Found:",
            duplicated_actigraphy_records,
        )

    ActD = pd.DataFrame(actdata, columns=["PIN"])
    inventoryaabc6 = pd.merge(aabc_inventory_6, ActD, on="PIN", how="left", indicator=True)
    inventoryaabc6["has_actigraphy_data"] = inventoryaabc6._merge != "left_only"

    qc_missing_actigraphy_data_in_box(inventoryaabc6)

    psychopy_code_block(inventoryaabc6)


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
        dbitems = list_files_in_box_folders(box_folder_id)
        save_cache()
        for fname in dbitems.filename:

            # TODO: exclude practice runs from snapshot like 'CARIT_HCA8860898_V3_run0_2022-09-21_111428_design.csv'
            search_result = re.search("(HCA\d+_V\d_[A-Z])", fname)
            if not search_result:
                print(fname)
                continue
            subjvscan = search_result.groups()[0]
            l2 = subjvscan.split("_")
            row = l2 + [fname]
            psychopy_files_list.append(row)

    pyschopy_files_df = pd.DataFrame(psychopy_files_list)

    pyschopy_files_df.columns = ["subject", "redcap_event", "scan", "fname"]
    checkIDB = pyschopy_files_df[["subject", "redcap_event", "scan"]]
    checkIDB["PIN_AB"] = checkIDB.subject + "_" + checkIDB.redcap_event + "_" + checkIDB.scan
    ci = checkIDB.drop_duplicates(subset="PIN_AB")

    # just check for existence of PsychoPY in IntraDB
    # /ceph/intradb/archive/AABC_WU_ITK/arc001/HCA7281271_V3_B/RESOURCES/LINKED_DATA/PSYCHOPY/
    psychointradb4 = list_psychopy_subjects("AABC_WU_ITK")
    psychointradb3 = list_psychopy_subjects("AABC_UMN_ITK")
    psychointradb2 = list_psychopy_subjects("AABC_UCLA_ITK")
    psychointradb1 = list_psychopy_subjects("AABC_MGH_ITK")

    df4 = pd.DataFrame(str.splitlines(psychointradb4))
    df4 = df4[0].str.split(",", expand=True)
    df3 = pd.DataFrame(str.splitlines(psychointradb3))
    df3 = df3[0].str.split(",", expand=True)
    # df2 = pd.DataFrame(str.splitlines(psychointradb2))
    # df2 = df2[0].str.split(',', expand=True)
    df1 = pd.DataFrame(str.splitlines(psychointradb1))
    df1 = df1[0].str.split(",", expand=True)

    df = pd.concat([df1, df3, df4], axis=0)  # df2,
    df.columns = ["PIN_AB"]
    df.PIN_AB = df.PIN_AB.str.replace("t", "")

    # merge df (intradb) and ci (Box) to see what's missing - one of these is redundant with the check against AABC...
    psymiss = pd.merge(ci, df, on="PIN_AB", how="outer", indicator=True)
    print("psychopy in BOX but not in IntraDB")
    for i in psymiss.loc[psymiss._merge == "left_only"].PIN_AB.unique():
        print(i)
    p1 = pd.DataFrame(psymiss.loc[psymiss._merge == "left_only"].PIN_AB.unique())
    p1["code"] = "ORANGE"
    p1["reason"] = "Psychopy Data Found in Box but not IntraDB"

    # p2=pd.DataFrame()
    p2 = []
    print("psychopy in IntraDB but not in Box")
    for i in psymiss.loc[psymiss._merge == "right_only"].PIN_AB.unique():
        print(i)
    p2 = pd.DataFrame(psymiss.loc[psymiss._merge == "right_only"].PIN_AB.unique())
    p2["code"] = "ORANGE"
    p2["reason"] = "Psychopy Data Found in IntraDB but not Box"

    p = pd.concat([p1, p2])  # ,columns='PIN_AB')
    p["PIN"] = p[0].str[:13]
    p["PIN_AB"] = p[0]
    p["subject_id"] = p[0].str[:10]
    p["subject"] = p[0].str[:10]
    pwho = pd.merge(
        inventoryaabc6.loc[is_v_event(inventoryaabc6, "redcap_event")].drop(columns=["subject_id", "subject"]),
        p,
        on="PIN",
        how="right",
    )
    pwho = pwho[
        [
            "subject",
            "subject_id",
            "study_id",
            "redcap_event",
            "redcap_event_name",
            "site",
            "reason",
            "code",
            "v0_date",
            "event_date",
            "PIN_AB",
        ]
    ]
    # TODO: register_ticket for pwho or better yet, the components of pwho (p1 and p2)

    # dont worry about duplicates in IntraDB - these will be filtered.
    # find subjects in AABC but not in IntraDB or BOX
    PSY2 = psymiss.drop_duplicates(subset="subject")[["subject", "redcap_event"]]
    PSY2["has_psychopy_data"] = True
    inventoryaabc7 = pd.merge(inventoryaabc6, PSY2, on=["subject", "redcap_event"], how="left", indicator=True)
    inventoryaabc7["has_psychopy_data"] = inventoryaabc7._merge != "left_only"
    cron_clean_up_aabc_inventory_for_recruitment_stats(inventoryaabc6, inventoryaabc7)
    vinventoryaabc7 = inventoryaabc7.loc[is_v_event(inventoryaabc7)].copy()
    qc_psychopy_not_found_in_box_or_intradb(vinventoryaabc7)
    qc_redcap_missing_counterbalance(inventoryaabc7)
    qc_hot_flash_data()
    qc_vns_data()
    qc_bunk_ids_in_psychopy_and_actigraphy()
    qc_visit_summary_incomplete(vinventoryaabc7)
    qc_age_in_v_events(vinventoryaabc7)
    qc_bmi_in_v_events(vinventoryaabc7)


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
