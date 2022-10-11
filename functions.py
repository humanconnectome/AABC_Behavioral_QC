import subprocess
from typing import List

from memoizable import Memoizable

import pandas as pd
import requests

from config import *


## get configuration files

config = LoadSettings()
tickets_dataframe = pd.DataFrame(
    columns=[
        "subject_id",
        "study_id",
        "redcap_event_name",
        "site",
        "reason",
        "code",
        "v0_date",
        "event_date",
    ]
)

# functions
def params_request_records(token):
    return {
        "token": token,
        "content": "record",
        "format": "json",
        "type": "flat",
        "csvDelimiter": "",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "json",
    }


def params_request_report(token, report_id):
    return {
        "token": token,
        "content": "report",
        "format": "json",
        "report_id": report_id,
        "csvDelimiter": "",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "returnFormat": "json",
    }


def get_frame(api_url: str, data: dict) -> pd.DataFrame:
    """Get a dataframe from a Redcap API call

    Args:
        data: dict of parameters for the API call
        api_url: url for the API call

    Returns:
        A dataframe of the results
    """
    r = requests.post(api_url, data=data)
    if r.status_code != 200:
        print(f"HTTP Status: {r.status_code}")
    return pd.DataFrame(r.json())


class RedcapFrameLoader(Memoizable):
    def run(self, api_url: str, data: dict) -> pd.DataFrame:
        """Get a dataframe from a Redcap API call

        Args:
            data: dict of parameters for the API call
            api_url: url for the API call

        Returns:
            A dataframe of the results
        """
        return get_frame(api_url, data)


memo_get_frame = RedcapFrameLoader(cache_file=".cache_redcap_frame", expire_in_days=1)


def idvisits(aabc_arms_df: pd.DataFrame) -> pd.DataFrame:
    """Prepares a dataframe by doing 3 things:
    1. Forward filling information (site, subject) from registration event to all other events
    2. Mapping the raw `redcap_event_name` to `redcap_event`

    Args:
        aabc_arms_df: Dataframe fresh from redcap
        keep_cols: Columns to keep in the dataframe

    Returns:
        A modified dataframe
    """
    df = aabc_arms_df.copy()
    # convert empty strings to NaN
    df.site = df.site.where(df.site != "")
    # Now forward fill the fresh NaNs
    df.site = df.site.ffill()

    # repeat process as above, but for 'subject_id'.
    #   but name the column 'subject' for some reason
    df["subject"] = df.subject_id.where(df.subject_id != "")
    df.subject = df.subject.ffill()

    df["redcap_event"] = df.redcap_event_name.replace(
        config["Redcap"]["datasources"]["aabcarms"]["AABCeventmap"]
    )
    return df


def concat(*args):
    return pd.concat([x for x in args if not x.empty], axis=0)


def parse_content(content):
    section_headers = [
        "Subtest,,Raw score",
        "Subtest,,Scaled score",
        "Subtest,Type,Total",  # this not in aging or RAVLT
        "Subtest,,Completion Time (seconds)",
        "Subtest,Type,Yes/No",
        "Item,,Raw score",
    ]
    # Last section header is repeat data except for RAVLT
    if "RAVLT" in content:
        section_headers.append("Scoring Type,,Scores")

    new_row = []
    capture_flag = False
    for row in content.splitlines():
        row = row.strip(' "')
        if row in section_headers:
            capture_flag = True

        elif row == "":
            capture_flag = False

        elif capture_flag:
            value = row.split(",")[-1].strip()

            if value == "-":
                value = ""
            new_row.append(value)

    return new_row


def send_frame(dataframe, tok):
    return None
    data = {
        "token": tok,
        "content": "record",
        "format": "csv",
        "type": "flat",
        "overwriteBehavior": "normal",
        "forceAutoNumber": "false",
        "data": dataframe.to_csv(index=False),
        "returnContent": "ids",
        "returnFormat": "json",
    }
    r = requests.post("https://redcap.wustl.edu/redcap/api/", data=data)
    print("HTTP Status: " + str(r.status_code))
    print(r.json())


def run_ssh_cmd(host: str, cmd: str) -> str:
    cmds = ["ssh", "-t", host, cmd]
    return (
        subprocess.check_output(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        .decode("utf-8")
        .strip()
    )


class SSHCmdRunner(Memoizable):
    def run(self, host: str, cmd: str) -> str:
        return run_ssh_cmd(host, cmd)


memo_run_ssh_cmd = SSHCmdRunner(cache_file=".cache_ssh_cmd", expire_in_days=8)


def get_list_from_excel_sheet(excel_file_path: str, sheet_name: str) -> List[str]:
    """Get a list of values from a specific sheet in an excel file

    Args:
        excel_file_path: path to the excel file
        sheet_name: name of the sheet to get the values from

    Returns:
        A list of field names from the sheet
    """
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    return df.field_name.to_list()


def TLBXreshape(results1):
    df = pd.DataFrame(str.splitlines(results1))
    df = df[0].str.split(",", expand=True)
    cols = df.loc[df[0] == "PIN"].values.tolist()
    df2 = df.loc[~(df[0] == "PIN")]
    df2.columns = cols[0]
    return df2


def filterdupass(instrument, dupvar, iset, dset):
    fixass = iset[
        [
            "subject",
            "subject_id",
            "study_id",
            "redcap_event",
            "redcap_event_name",
            "site",
            "v0_date",
            "event_date",
            dupvar,
        ]
    ].copy()
    fixass["reason"] = "Duplicated Assessments"
    fixass["code"] = "orange"
    fixass["PIN"] = fixass.subject + "_" + fixass.redcap_event
    fixass = fixass.loc[~(fixass[dupvar] == "")][["PIN", dupvar]]
    fixass["Assessment Name"] = "Assessment " + fixass[dupvar]
    fixass["Inst"] = instrument
    dset = pd.merge(dset, fixass, on=["PIN", "Inst", "Assessment Name"], how="left")
    dset = dset.loc[~(dset[dupvar].isnull() == False)]
    return dset


def print_error_codes(df: pd.DataFrame) -> None:
    """Print error codes from a dataframe

    Args:
        df: dataframe to print error codes from
    """
    for row in df.itertuples():
        print(f"CODE {row.code}: {row.subject_id}: {row.reason}")


def register_tickets(df, code: str, reason: str, error_code: str = "AE0000") -> None:
    """Register new tickets in the tickets dataframe

    Args:
        df: The dataframe containing all the rows to register
        code: The code for the ticket
        reason: The description of the error

    """
    global tickets_dataframe
    n = df.copy()
    n["issueCode"] = error_code
    n["code"] = code
    n["reason"] = reason

    rename_col(n, "subject_id", "subject")
    rename_col(n, "redcap_event_name", "redcap_event")

    print_error_codes(n)
    tickets_dataframe = pd.concat([tickets_dataframe, n], ignore_index=True)


def rename_col(df, preferred_field_name, current_field_name):
    """Rename a column in a dataframe

    Args:
        df: dataframe to rename column in
        preferred_field_name: the preferred name for the column
        current_field_name: the current name for the column
    """
    if preferred_field_name not in df.columns and current_field_name in df.columns:
        df.rename(columns={current_field_name: preferred_field_name}, inplace=True)


def get_aabc_arms_report(token) -> pd.DataFrame:
    """Get the AABC arms report from REDCap

    Returns:
        A dataframe of the report
    """
    aabc_arms_report_request = params_request_report(
        token=token,
        report_id="51031",
    )
    df = memo_get_frame(
        api_url=config["Redcap"]["api_url10"], data=aabc_arms_report_request
    )
    return df


def remove_test_subjects(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Remove test subjects from a dataframe

    Args:
        df: dataframe to remove test subjects from
        field: field to check for test subjects

    Returns:
        A dataframe with test subjects removed
    """
    return df.loc[~df[field].str.contains("test", na=False, case=False)].copy()


def is_register_event(df: pd.DataFrame) -> pd.Series:
    """Check if the event is the register event

    Args:
        df: dataframe to check

    Returns:
        A series of booleans
    """
    return df.redcap_event_name.str.contains("register", case=False, na=False)


def cat_toolbox_score_files(proj):
    return memo_run_ssh_cmd(
        "chpc3",
        f'cat /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u',
    )


def cat_toolbox_rawdata_files(proj):
    return memo_run_ssh_cmd(
        "chpc3",
        f'find /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {{}} \;',
    )


def list_psychopy_subjects(proj):
    return memo_run_ssh_cmd(
        "chpc3",
        f"ls /ceph/intradb/archive/{proj}/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u",
    )


def qc_detect_test_subjects_in_production_database(prod_df: pd.DataFrame) -> None:
    """Detects test subjects in production database and raises a ticket if found"""
    test_subjects = prod_df.loc[
        prod_df["subject_id"].str.contains("test", case=False),
        [
            "subject_id",
            "study_id",
            "redcap_event_name",
            "site",
            "v0_date",
            "event_date",
        ],
    ]
    register_tickets(
        test_subjects,
        "HOUSEKEEPING",
        "HOUSEKEEPING : Please delete test subject.  Use test database when practicing",
        "AE6001",
    )


def qc_subjects_found_in_aabc_not_in_hca(
    aabc_inventory: pd.DataFrame, hca_inventory: pd.DataFrame
) -> None:
    aabc_registration_data = aabc_inventory.loc[
        # Redcap only stores form one data (ids and legacy information) in the initial "register" event (V0)
        is_register_event(aabc_inventory),
        # fields of interest from form one
        [
            "study_id",
            "redcap_event_name",
            "subject_id",
            "legacy_yn",
            "site",
            "v0_date",
        ],
    ]
    # Merge to compare AABC ids against HCA ids
    #  - also check legacy variable flags and actual event in which participant has been enrolled.
    hca_unique_subject_ids = hca_inventory.subject.drop_duplicates()
    hca_vs_aabc = pd.merge(
        hca_unique_subject_ids,
        aabc_registration_data,
        left_on="subject",
        right_on="subject_id",
        how="outer",
        indicator=True,
    )
    legacy_arms = [
        "register_arm_1",
        "register_arm_2",
        "register_arm_3",
        "register_arm_4",
        "register_arm_5",
        "register_arm_6",
        "register_arm_7",
        "register_arm_8",
    ]
    # Boolean filters
    is_legacy_id = hca_vs_aabc.redcap_event_name.isin(legacy_arms) | (
        hca_vs_aabc.legacy_yn == "1"
    )
    is_in_aabc_not_in_hca = hca_vs_aabc._merge == "right_only"
    is_in_both_hca_aabc = hca_vs_aabc._merge == "both"
    # First batch of flags: Look for legacy IDs that don't actually exist in HCA
    # send these to Angela for emergency correction:
    cols_for_troubleshooting = [
        "subject_id",
        "study_id",
        "redcap_event_name",
        "site",
        "v0_date",
    ]
    qlist1 = hca_vs_aabc.loc[
        is_in_aabc_not_in_hca & is_legacy_id & hca_vs_aabc["subject_id"].notnull(),
        cols_for_troubleshooting,
    ]
    register_tickets(
        qlist1,
        "RED",
        "Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list",
        "AE1001",
    )
    # 2nd batch of flags: if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
    qlist2 = hca_vs_aabc.loc[
        is_in_both_hca_aabc & ~is_legacy_id, cols_for_troubleshooting
    ]
    register_tickets(
        qlist2,
        "RED",
        "Subject found in AABC REDCap Database with an ID from HCP-A study but no legacyYN not checked",
        "AE1001",
    )


def qc_subject_id_is_not_missing(aabc_inventory):
    missing_sub_ids = aabc_inventory.loc[
        is_register_event(aabc_inventory) & (aabc_inventory["subject_id"] == "")
    ]
    qlist4 = missing_sub_ids[
        [
            "subject_id",
            "study_id",
            "redcap_event_name",
            "site",
            "reason",
            "code",
            "v0_date",
            "event_date",
        ]
    ]
    register_tickets(
        qlist4,
        "ORANGE",
        "Subject ID is MISSING in AABC REDCap Database Record with study id",
        "AE1001",
    )


def qc_subject_initiating_wrong_visit_sequence(aabc_inventory, hca_inventory):
    # if legacy v1 and enrolled as if v3 or v4 or legacy v2 and enrolled v4
    aabc_id_visits = aabc_inventory.sort_values(["study_id", "redcap_event_name"])
    aabc_nonregister_visits = aabc_id_visits.loc[
        ~is_register_event(aabc_id_visits),
        [
            "study_id",
            "redcap_event_name",
            "site",
            "subject_id",
            "v0_date",
            "event_date",
        ],
    ]
    # dataframe contains the last visit (`redcap_event`) for each subject (`subject`). Will be used:
    # - to check participant is not enrolled in wrong arm
    # - to check participant is starting correct next visit
    hca_last_visits = (
        hca_inventory[["subject", "redcap_event"]]
        .loc[hca_inventory.redcap_event.isin(["V1", "V2"])]
        .sort_values("redcap_event")
        .drop_duplicates(subset="subject", keep="last")
    )
    # Increment the last visit by 1 to get the next visit
    next_visit = hca_last_visits.redcap_event.str.replace("V", "").astype("int") + 1
    hca_last_visits["next_visit2"] = "V" + next_visit.astype(str)
    hca_last_visits2 = hca_last_visits.drop(columns=["redcap_event"])
    # check that current visit in AABC is the last visit in HCA + 1
    hca_expected_vs_aabc_actual = pd.merge(
        hca_last_visits2,
        aabc_nonregister_visits,
        left_on=["subject", "next_visit2"],
        right_on=["subject", "redcap_event"],
        how="right",
        indicator=True,
    )
    wrong_visit = hca_expected_vs_aabc_actual.loc[
        # was in actual but not expected
        (hca_expected_vs_aabc_actual._merge == "right_only")
        # and was not a phone call event
        & (hca_expected_vs_aabc_actual.redcap_event_name != "phone_call_arm_13"),
        [
            "subject_id",
            "study_id",
            "redcap_event_name",
            "site",
            "event_date",
        ],
    ]
    register_tickets(
        wrong_visit,
        "RED",
        "Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2",
        "AE1001",
    )


def qc_unable_to_locate_qint_data(aabc_inventory_plus_qint, aabc_vs_qint):
    missingQ = aabc_inventory_plus_qint.loc[
        aabc_vs_qint.redcap_event_name.str.contains("v") & ~aabc_vs_qint.has_qint_data,
        ["subject_id", "study_id", "subject", "redcap_event", "site", "event_date"],
    ]
    register_tickets(
        missingQ,
        "ORANGE",
        "Unable to locate Q-interactive data for this subject/visit",
        "AE1001",
    )


def qc_has_qint_but_id_visit_not_found_in_aabc(aabc_vs_qint):
    qint_only = aabc_vs_qint.loc[aabc_vs_qint._merge == "right_only"]
    register_tickets(
        qint_only[["subject", "redcap_event"]],
        "ORANGE",
        "Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo",
        "AE1001",
    )


def qc_duplicate_qint_records(qint_df):
    dups = qint_df.loc[qint_df.duplicated(subset=["subjectid", "visit"])]
    dups2 = dups.loc[~(dups.q_unusable.isnull() == False)]  # or '', not sure
    register_tickets(
        dups2,
        "ORANGE",
        "Duplicate Q-interactive records",
        "AE5001",
    )


def qc_raw_or_scored_data_not_found(dffull, rf2):
    # QC check:
    # Either scored or raw is missing in format expected:
    formats = pd.merge(
        dffull.PIN.drop_duplicates(), rf2, how="outer", on="PIN", indicator=True
    )[["PIN", "_merge"]]
    issues = formats.loc[~(formats._merge == "both")]
    register_tickets(
        issues,
        "ORANGE",
        "Raw or Scored data not found (make sure you didn't export Narrow format)",
        "AE5001",
    )


def qc_toolbox_pins_not_in_aabc(pre_aabc_inventory_5):
    # find toolbox records that aren't in AABC - typos are one thing...legit ids are bad because don't know which one is right unless look at date, which is missing for cog comps
    # turn this into a ticket
    t2 = pre_aabc_inventory_5.loc[
        pre_aabc_inventory_5._merge == "right_only", ["PIN", "subject", "redcap_event"]
    ]
    register_tickets(
        t2,
        "ORANGE",
        "TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?",
        "AE1001",
    )


def qc_missing_tlbx_data(aabc_inventory_5):
    # Look for missing IDs
    missingT = aabc_inventory_5.loc[
        aabc_inventory_5.redcap_event_name.str.contains("v")
        & ~aabc_inventory_5.has_tlbx_data
    ]
    t3 = missingT[
        [
            "subject",
            "redcap_event",
            "site",
            "event_date",
            "nih_toolbox_collectyn",
        ]
    ]
    register_tickets(
        t3,
        "ORANGE",
        "Missing TLBX data",
        "AE2001",
    )


def qc_unable_to_locate_asa24_id_in_redcap_or_box(aabc_inventory_6):
    missingAD = aabc_inventory_6.loc[
        aabc_inventory_6.redcap_event_name.str.contains("v")
        & ~aabc_inventory_6.has_asa24_data
    ]
    missingAD = missingAD.loc[~(missingAD.asa24yn == "0")]
    a1 = missingAD[
        [
            "subject_id",
            "subject",
            "study_id",
            "redcap_event",
            "redcap_event_name",
            "site",
            "reason",
            "code",
            "v0_date",
            "event_date",
            "asa24yn",
            "asa24id",
        ]
    ]
    register_tickets(
        a1,
        "GREEN",
        "Unable to locate ASA24 id in Redcap or ASA24 data in Box for this subject/visit",
        "AE2001",
    )


def qc_missing_actigraphy_data_in_box(inventoryaabc6):
    # Missing?
    missingAct = inventoryaabc6.loc[
        inventoryaabc6.redcap_event_name.str.contains("v")
        & ~inventoryaabc6.has_actigraphy_data
    ]
    missingAct = missingAct.loc[~(missingAct.actigraphy_collectyn == "0")]
    a2 = missingAct[
        [
            "subject_id",
            "subject",
            "redcap_event",
            "study_id",
            "redcap_event_name",
            "site",
            "v0_date",
            "event_date",
            "actigraphy_collectyn",
        ]
    ]
    register_tickets(
        a2,
        "YELLOW",
        "Unable to locate Actigraphy data in Box for this subject/visit",
        "AE4001",
    )
