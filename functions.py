import pandas as pd
import requests
from subprocess import Popen, PIPE
from config import *


## get configuration files
config = LoadSettings()

# functions
def redjson(tok):
    aabcarms = {
        "token": tok,
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
    return aabcarms


def redreport(tok, reportid):
    aabcreport = {
        "token": tok,
        "content": "report",
        "format": "json",
        "report_id": reportid,
        "csvDelimiter": "",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "returnFormat": "json",
    }
    return aabcreport


def getframe(struct, api_url):
    r = requests.post(api_url, data=struct)
    print("HTTP Status: " + str(r.status_code))
    a = r.json()
    HCAdf = pd.DataFrame(a)
    return HCAdf


def idvisits(aabcarmsdf, keepsies):
    idvisit = aabcarmsdf[keepsies].copy()
    registers = idvisit.loc[idvisit.redcap_event_name.str.contains("register")][
        ["subject_id", "study_id", "site"]
    ]
    idvisit = pd.merge(
        registers, idvisit.drop(columns=["site"]), on="study_id", how="right"
    )
    idvisit = idvisit.rename(
        columns={"subject_id_x": "subject", "subject_id_y": "subject_id"}
    )
    idvisit["redcap_event"] = idvisit.replace(
        {
            "redcap_event_name": config["Redcap"]["datasources"]["aabcarms"][
                "AABCeventmap"
            ]
        }
    )["redcap_event_name"]
    idvisit = idvisit.loc[
        ~(idvisit.subject.astype(str).str.upper().str.contains("TEST"))
    ]
    return idvisit


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


def run_ssh_cmd(host, cmd):
    cmds = ["ssh", "-t", host, cmd]
    return Popen(cmds, stdout=PIPE, stderr=PIPE, stdin=PIPE)


def getlist(mask, sheet):
    restrictA = pd.read_excel(mask, sheet_name=sheet)
    restrictedA = list(restrictA.field_name)
    return restrictedA


def TLBXreshape(results1):
    df = results1.decode("utf-8")
    df = pd.DataFrame(str.splitlines(results1.decode("utf-8")))
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
