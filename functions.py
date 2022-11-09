import subprocess
from datetime import date
from typing import List

from memofn import memofn

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
    print("Getting", data)
    r = requests.post(api_url, data=data)
    if r.status_code != 200:
        print(f"HTTP Status: {r.status_code}")
    return pd.DataFrame(r.json())


memo_get_frame = memofn(get_frame, expire_in_days=1)


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

    df["redcap_event"] = df.redcap_event_name.replace(config["Redcap"]["datasources"]["aabcarms"]["AABCeventmap"])
    return df


def concat(*args):
    return pd.concat([x for x in args if not x.empty], axis=0)


example_content = """,,An asterisk represents scores that were derived from unresolved flagged items.
,,A blank represents subtests that were not administered.
,,A dash represents subtests that were administered but could not be scored.


RAW SCORES

Subtest,,Raw score
RAVLT-Alternate Form C Short Delay,,45

SCALED SCORES

Subtest,,Scaled score
RAVLT-Alternate Form C Short Delay,,-

CONTEXTUAL EVENTS

Subtest,Type,Total

SUBTEST COMPLETION TIMES

Subtest,,Completion Time (seconds)
RAVLT-Alternate Form C Short Delay,,431

RULES TRIGGERED

Subtest,Type,Yes/No
RAVLT-Alternate Form C Short Delay, Discontinue, No
RAVLT-Alternate Form C Short Delay, Reverse, No


Z SCORES

Item,Score

Z SCORES: ITEM LEVEL SCORES

Item,Score

ITEM-LEVEL RAW SCORES

Item,,Raw score
"RAVLT-Alternate Form C Short Delay Trial I",,4
"RAVLT-Alternate Form C Short Delay Trial II",,9
"RAVLT-Alternate Form C Short Delay Trial III",,8
"RAVLT-Alternate Form C Short Delay Trial IV",,9
"RAVLT-Alternate Form C Short Delay Trial V",,9
"RAVLT-Alternate Form C Short Delay List B Trial",,2
"RAVLT-Alternate Form C Short Delay Trial VI",,4

Additional Measures (Primary and Combined and Process)

Scoring Type,,Scores
Trial 1 Free Recall Total Correct,,-
Trial 2 Free Recall Total Correct,,-
Trial 3 Free Recall Total Correct,,-
Trial 4 Free Recall Total Correct,,-
Short-Delay Free Recall Total Correct,,-
Short Delay Free-Recall Intrusions,,0
Short Delay Total Intrusions,,0
Short Delay Total Repetitions,,2

Composite Score

Name,

"""


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
    cmds = ["ssh", host, cmd]
    return subprocess.check_output(cmds).decode("utf-8").strip()


memo_run_ssh_cmd = memofn(run_ssh_cmd, expire_in_days=8)


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


@memofn(expire_in_days=8)
def toolbox_to_dataframe(text_content):
    csv = [line.split(",") for line in text_content.strip().splitlines()]
    df = pd.DataFrame(csv)

    # First row always contains headers
    # Promote it to column names
    df.columns = df.iloc[0]

    # Drop all rows with headers
    df = df.loc[df.PIN != "PIN"]

    return df


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


def register_tickets(df, code: str, reason: str, error_code: str = "AE0000", critical: bool = False) -> None:
    """Register new tickets in the tickets dataframe

    Args:
        df: The dataframe containing all the rows to register
        code: The code for the ticket
        reason: The description of the error
        critical: If True, send an email to Angela and Petra, ASAP

    """
    # TODO: implement `critical` param
    global tickets_dataframe
    n = df.copy()
    n["issueCode"] = error_code
    n["code"] = code
    n["reason"] = reason
    today = pd.to_datetime(date.today().strftime("%Y-%m-%d"))
    n["issue_age"] = today - pd.to_datetime(n.event_date)

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


def get_aabc_arms_report(token, report_id="51031") -> pd.DataFrame:
    """Get the AABC arms report from REDCap

    Returns:
        A dataframe of the report
    """
    aabc_arms_report_request = params_request_report(
        token=token,
        report_id=report_id,
    )
    df = memo_get_frame(api_url=config["Redcap"]["api_url10"], data=aabc_arms_report_request)
    return df


def remove_test_subjects(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Remove test subjects from a dataframe

    Args:
        df: dataframe to remove test subjects from
        field: field to check for test subjects

    Returns:
        A dataframe with test subjects removed
    """
    return df.loc[
        ~df[field].str.contains("test", na=False, case=False)
        # looking for anything is a test but of the form "ABC123"
        & (~df[field].str.contains("B"))
    ].copy()


def is_v_event(df: pd.DataFrame, field: str = "redcap_event_name") -> pd.Series:
    """Check if "v" is in the field.

    Args:
        df: Dataframe to use
        field: Default is "redcap_event_name" #TODO: probably needs to be removed, used only once

    Returns:
        A series of booleans
    """
    return df[field].str.contains("v", na=False, case=False)


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
        f'find /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/*Scores* -type f ! \( -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {{}} \;',
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


def qc_detect_test_subjects_in_production_database(
    prod_df: pd.DataFrame,
) -> None:
    """Detects test subjects in production database and raises a ticket if found"""
    test_subjects = prod_df.loc[prod_df["subject_id"].str.contains("test", case=False)]
    register_tickets(
        test_subjects,
        "HOUSEKEEPING",
        "HOUSEKEEPING : Please delete test subject.  Use test database when practicing",
        "AE6001",
    )


def qc_subjects_found_in_aabc_not_in_hca(aabc_inventory: pd.DataFrame, hca_inventory: pd.DataFrame) -> None:
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
            "event_date",
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
    is_legacy_id = hca_vs_aabc.redcap_event_name.isin(legacy_arms) | (hca_vs_aabc.legacy_yn == "1")
    is_in_aabc_not_in_hca = hca_vs_aabc._merge == "right_only"
    is_in_both_hca_aabc = hca_vs_aabc._merge == "both"

    qlist1 = hca_vs_aabc.loc[
        is_in_aabc_not_in_hca & is_legacy_id & hca_vs_aabc["subject_id"].notnull(),
    ]
    register_tickets(
        qlist1,
        "RED",
        "Subject found in AABC REDCap Database with legacy indications whose ID was not found in HCP-A list",
        "AE1001",
    )

    qlist2 = hca_vs_aabc.loc[is_in_both_hca_aabc & ~is_legacy_id]
    register_tickets(
        qlist2,
        "RED",
        "Subject found in AABC REDCap Database with an ID from HCP-A study but no legacyYN not checked",
        "AE1001",
    )


def qc_subject_id_is_not_missing(aabc_inventory):
    qlist4 = aabc_inventory.loc[is_register_event(aabc_inventory) & (aabc_inventory["subject_id"] == "")]
    register_tickets(qlist4, "ORANGE", "Subject ID is MISSING in AABC REDCap Database Record with study id", "AE1001")


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
            "subject",
            "redcap_event",
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
        & (hca_expected_vs_aabc_actual.redcap_event_name != "phone_call_arm_13")
    ]
    register_tickets(
        wrong_visit,
        "RED",
        "Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2",
        "AE1001",
        critical=True,
    )


def qc_unable_to_locate_qint_data(aabc_inventory_plus_qint, aabc_vs_qint):
    missingQ = aabc_inventory_plus_qint.loc[
        is_v_event(aabc_vs_qint) & ~aabc_vs_qint.has_qint_data,
        [
            "subject_id",
            "study_id",
            "subject",
            "redcap_event",
            "site",
            "event_date",
        ],
    ]
    register_tickets(missingQ, "ORANGE", "Unable to locate Q-interactive data for this subject/visit", "AE1001")


def qc_has_qint_but_id_visit_not_found_in_aabc(aabc_vs_qint):
    qint_only = aabc_vs_qint.loc[aabc_vs_qint._merge == "right_only"]
    # TODO: Use what event date? (for calculating `issue_age`)
    qint_only["event_date"] = date.today().strftime("%Y-%m-%d")
    register_tickets(
        qint_only[["subject", "redcap_event", "event_date"]],
        "ORANGE",
        "Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo",
        "AE1001",
    )


def qc_duplicate_qint_records(qint_df):
    dups = qint_df.loc[qint_df.duplicated(subset=["subjectid", "visit"])]
    if dups.empty:
        return
    dups2 = dups.loc[dups.q_unusable == ""]
    # TODO: event_date is required, but need to double-check with Petra that this is acceptable date to use
    dups2["event_date"] = dups2.created
    register_tickets(dups2, "ORANGE", "Duplicate Q-interactive records", "AE5001")


def qc_raw_or_scored_data_not_found(dffull, rf2):
    # QC check:
    # Either scored or raw is missing in format expected:
    formats = pd.merge(dffull.PIN.drop_duplicates(), rf2, how="outer", on="PIN", indicator=True)[["PIN", "_merge"]]
    issues = formats.loc[~(formats._merge == "both")]
    register_tickets(
        issues, "ORANGE", "Raw or Scored data not found (make sure you didn't export Narrow format)", "AE5001"
    )


def qc_toolbox_pins_not_in_aabc(pre_aabc_inventory_5):
    # find toolbox records that aren't in AABC - typos are one thing...legit ids are bad because don't know which one is right unless look at date, which is missing for cog comps
    # turn this into a ticket
    t2 = pre_aabc_inventory_5.loc[
        pre_aabc_inventory_5._merge == "right_only",
        ["PIN", "subject", "redcap_event"],
    ]
    register_tickets(t2, "ORANGE", "TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?", "AE1001")


def qc_missing_tlbx_data(aabc_inventory_5):
    # Look for missing IDs
    missingT = aabc_inventory_5.loc[is_v_event(aabc_inventory_5) & ~aabc_inventory_5.has_tlbx_data]
    t3 = missingT[["subject", "redcap_event", "site", "event_date", "nih_toolbox_collectyn"]]
    register_tickets(t3, "ORANGE", "Missing TLBX data", "AE2001")


def qc_unable_to_locate_asa24_id_in_redcap_or_box(aabc_inventory_6):
    missingAD = aabc_inventory_6.loc[is_v_event(aabc_inventory_6) & ~aabc_inventory_6.has_asa24_data]
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
        a1, "GREEN", "Unable to locate ASA24 id in Redcap or ASA24 data in Box for this subject/visit", "AE2001"
    )


def qc_missing_actigraphy_data_in_box(inventoryaabc6):
    # Missing?
    missingAct = inventoryaabc6.loc[is_v_event(inventoryaabc6) & ~inventoryaabc6.has_actigraphy_data]
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
    register_tickets(a2, "YELLOW", "Unable to locate Actigraphy data in Box for this subject/visit", "AE4001")


def qc_psychopy_not_found_in_box_or_intradb(inventoryaabc7):
    missingPY = inventoryaabc7.loc[
        ~inventoryaabc7.has_psychopy_data,
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "reason",
            "code",
            "v0_date",
            "event_date",
            "has_psychopy_data",
        ],
    ]
    register_tickets(missingPY, "ORANGE", "PsychoPy cannot be found in BOX or IntraDB", "AE4001")


def qc_redcap_missing_counterbalance(inventoryaabc7):
    cb = inventoryaabc7.loc[
        is_register_event(inventoryaabc7) & (inventoryaabc7.counterbalance_2nd == ""),
        [
            "site",
            "study_id",
            "redcap_event",
            "redcap_event_name",
            "subject",
            "v0_date",
            "passedscreen",
        ],
    ]
    register_tickets(cb, "RED", "Currently Missing Counterbalance", "AE3001")


def qc_visit_summary_incomplete(vinventoryaabc7):
    summv = vinventoryaabc7[
        [
            "study_id",
            "site",
            "subject",
            "redcap_event",
            "visit_summary_complete",
            "event_date",
        ]
    ]
    summv = summv.loc[~(summv.visit_summary_complete == "2")]
    register_tickets(summv, "GREEN", "Visit Summary Incomplete", "AE2001")


def qc_missing_age(agev):
    ageav2 = agev.loc[
        (agev.age_visit.astype(float).isnull() == True),
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "reason",
            "code",
            "event_date",
            "v0_date",
        ],
    ]
    register_tickets(ageav2, "RED", "Missing Age. Please check DOB and Event Date", "AE3001")


def qc_age_outlier(agev):
    ag = agev.loc[agev.age_visit != ""]
    agemv = ag.loc[
        (ag.age_visit.astype("float") <= 40) | (ag.age_visit.astype("float") >= 90),
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "reason",
            "code",
            "event_date",
            "v0_date",
        ],
    ]
    register_tickets(agemv, "RED", "Age outlier. Please double check DOB and Event Date", "AE7001")


def qc_missing_weight_or_height(bmiv):
    # missings
    bmiv2 = bmiv.loc[
        bmiv.bmi == "",
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "event_date",
        ],
    ]
    register_tickets(
        bmiv2, "RED", "Missing Height or Weight (or there is another typo preventing BMI calculation)", "AE3001"
    )


def qc_bmi_outlier(bmiv):
    # outliers
    a = bmiv.loc[bmiv.bmi != ""].copy()
    a = a.loc[
        (a.bmi.astype("float") <= 19) | (a.bmi.astype("float") >= 37),
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "event_date",
        ],
    ]
    register_tickets(a, "RED", "BMI is an outlier.  Please double check height and weight", "AE7001")


def qc_hot_flash_data():
    # TODO: qc hot flash data (data is not yet available)
    pass


def qc_vns_data():
    # TODO: qc vns data (data is not yet available)
    pass


def qc_bunk_ids_in_psychopy_and_actigraphy():
    # To DO: Forgot to CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY
    pass


def qc_age_in_v_events(vinventoryaabc7):
    agev = vinventoryaabc7[
        [
            "redcap_event",
            "study_id",
            "site",
            "subject",
            "redcap_event_name",
            "age_visit",
            "event_date",
            "v0_date",
        ],
    ]
    qc_age_outlier(agev)
    qc_missing_age(agev)


def qc_bmi_in_v_events(vinventoryaabc7):
    bmiv = vinventoryaabc7[
        ["bmi", "redcap_event", "subject", "study_id", "site", "event_date"],
    ].copy()
    qc_bmi_outlier(bmiv)
    qc_missing_weight_or_height(bmiv)


def cron_clean_up_aabc_inventory_for_recruitment_stats(inventoryaabc6, inventoryaabc7):
    # clean up the AABC inventory and upload to BOX for recruitment stats.
    inventoryaabc7.loc[inventoryaabc7.age == "", "age"] = inventoryaabc6.age_visit
    inventoryaabc7.loc[inventoryaabc7.event_date == "", "event_date"] = inventoryaabc7.v0_date
    inventoryaabc7 = inventoryaabc7.sort_values(["redcap_event", "event_date"])
    inventoryaabc7[
        [
            "study_id",
            "redcap_event_name",
            "redcap_event",
            "subject",
            "site",
            "age",
            "sex",
            "event_date",
            "passedscreen",
            "counterbalance_1st",
            "has_qint_data",
            "ravlt_collectyn",
            "has_tlbx_data",
            "nih_toolbox_collectyn",
            "nih_toolbox_upload_typo",
            "has_asa24_data",
            "asa24yn",
            "asa24id",
            "has_actigraphy_data",
            "actigraphy_collectyn",
            "vms_collectyn",
            "legacy_yn",
            "psuedo_guid",
            "ethnic",
            "racial",
            "visit_summary_complete",
        ]
    ].to_csv("Inventory_Beta.csv", index=False)
