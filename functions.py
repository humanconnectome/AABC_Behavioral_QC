import subprocess
from typing import List

import pandas as pd
import requests
from subprocess import Popen, PIPE

from InventoryDatatypes import api_key, config, memo_box_list_of_files
from config import *


## get configuration files
from memoizable import Memoizable

config = LoadSettings()

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


def get_aabc_arms_report() -> pd.DataFrame:
    """Get the AABC arms report from REDCap

    Returns:
        A dataframe of the report
    """
    aabc_arms_report_request = params_request_report(
        token=api_key["aabcarms"],
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


def list_files_in_box_folders(*box_folder_ids) -> pd.DataFrame:
    """List filename, fileid, sha1 for all files in specific box folders

    Args:
        *box_folder_ids: The box id for the folder of interest

    Returns:
        A dataframe with filename, fileid, sha1 for all files in the folder(s)

    """
    return memo_box_list_of_files(box_folder_ids)
