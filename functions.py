import subprocess
from typing import List

import pandas as pd
import requests
from subprocess import Popen, PIPE
from config import *


## get configuration files
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


def idvisits(aabc_arms_df: pd.DataFrame, keep_cols: List[str]) -> pd.DataFrame:
    """Prepares a dataframe by doing 3 things:
    1. Keeping only the columns specified in keep_cols
    2. Forward filling information (site, subject) from registration event to all other events
    3. Mapping the raw `redcap_event_name` to `redcap_event`

    Args:
        aabc_arms_df: Dataframe fresh from redcap
        keep_cols: Columns to keep in the dataframe

    Returns:
        A modified dataframe
    """
    df = aabc_arms_df[keep_cols].copy()
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


def run_ssh_cmd(host: str, cmd: str):
    cmds = ["ssh", "-t", host, cmd]
    return (
        subprocess.check_output(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        .decode("utf-8")
        .strip()
    )


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
