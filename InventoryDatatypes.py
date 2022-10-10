import collections
import re
from datetime import date
import pandas as pd

from ccf.box import LifespanBox
import functions
from config import LoadSettings

config = LoadSettings()
secret = pd.read_csv(config["config_files"]["secrets"])
api_key = secret.set_index("source")["api_key"].to_dict()
box = LifespanBox(cache="./tmp")


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


## get the HCA inventory for ID checking with AABC
csv_file_hca_inventory = box.download_file(config["hcainventory"])
hca_inventory = pd.read_csv(csv_file_hca_inventory)
hca_unique_subject_ids = hca_inventory.subject.drop_duplicates()

# dataframe contains the last visit (`redcap_event`) for each subject (`subject`). Will be used:
# - to check participant is not enrolled in wrong arm
# - to check participant is starting correct next visit
hca_last_visits = (
    hca_inventory[["subject", "redcap_event"]]
    .loc[hca_inventory.redcap_event.isin(["V1", "V2"])]
    .sort_values("redcap_event")
    .drop_duplicates(subset="subject", keep="last")
)


#########################################################################################
# PHASE 0 TEST IDS AND ARMS
# if Legacy, id exists in HCA and other subject id related tests:
# Test that #visits in HCA corresponds with cohort in AABC


def get_aabc_arms_report() -> pd.DataFrame:
    """Get the AABC arms report from REDCap

    Returns:
        A dataframe of the report
    """
    aabc_arms_report_request = functions.params_request_report(
        token=api_key["aabcarms"],
        report_id="51031",
    )
    df = functions.get_frame(
        api_url=config["Redcap"]["api_url10"], data=aabc_arms_report_request
    )
    return df


# download the inventory report from AABC for comparison
aabc_inventory_including_test_subjects = get_aabc_arms_report()

# trying to set study_id from config file, but have been sloppy...there are instances where the actual subject_id has been coded below
study_primary_key_field = config["Redcap"]["datasources"]["aabcarms"]["redcapidvar"]


def remove_test_subjects(df: pd.DataFrame, field: str) -> pd.DataFrame:
    """Remove test subjects from a dataframe

    Args:
        df: dataframe to remove test subjects from
        field: field to check for test subjects

    Returns:
        A dataframe with test subjects removed
    """
    return df.loc[~df[field].str.contains("test", na=False, case=False)].copy()


aabc_inventory = remove_test_subjects(
    aabc_inventory_including_test_subjects, study_primary_key_field
)
aabc_inventory = functions.idvisits(aabc_inventory)


def is_register_event(df: pd.DataFrame) -> pd.Series:
    """Check if the event is the register event

    Args:
        df: dataframe to check

    Returns:
        A series of booleans
    """
    return df.redcap_event_name.str.contains("register", case=False, na=False)


aabc_registration_data = aabc_inventory.loc[
    # Redcap only stores form one data (ids and legacy information) in the initial "register" event (V0)
    is_register_event(aabc_inventory),
    # fields of interest from form one
    [
        "study_id",
        "redcap_event_name",
        study_primary_key_field,
        "legacy_yn",
        "site",
        "v0_date",
    ],
]


def cat_toolbox_score_files(proj):
    return functions.run_ssh_cmd(
        "plenzini@login3.chpc.wustl.edu",
        f'cat /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/*Scores* | cut -d"," -f1,2,3,4,10 | sort -u',
    )


proj = "AABC_WU_ITK"
print(
    f'find /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {{}} \;'
)


def cat_toolbox_rawdata_files(proj):
    return functions.run_ssh_cmd(
        "plenzini@login3.chpc.wustl.edu",
        f'find /ceph/intradb/archive/{proj}/resources/toolbox_endpoint_data/ -type f  ! \( -name "*Scores*" -o -name "*Narrow*" -o -name "*Regist*" -o -name "*catalog*" \) -exec cat {{}} \;',
    )


def list_psychopy_subjects(proj):
    return functions.run_ssh_cmd(
        "plenzini@login3.chpc.wustl.edu",
        f"ls /ceph/intradb/archive/{proj}/arc001/*/RESOURCES/LINKED_DATA/PSYCHOPY/ | cut -d'_' -f2,3,4 | grep HCA | grep -E -v 'ITK|Eye|tt' | sort -u",
    )


def code_block_1() -> pd.DataFrame:
    # Merge to compare AABC ids against HCA ids
    #  - also check legacy variable flags and actual event in which participant has been enrolled.
    hca_vs_aabc = pd.merge(
        hca_unique_subject_ids,
        aabc_registration_data,
        left_on="subject",
        right_on=study_primary_key_field,
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
        is_in_aabc_not_in_hca
        & is_legacy_id
        & hca_vs_aabc[study_primary_key_field].notnull(),
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
    qlist3 = wrong_visit[
        [
            "subject_id",
            "study_id",
            "redcap_event_name",
            "site",
            "event_date",
        ]
    ]
    register_tickets(
        qlist3,
        "RED",
        "Subject found in AABC REDCap Database initiating the wrong visit sequence (e.g. V3 insteady of V2",
        "AE1001",
    )

    # check to make sure that the subject id is not missing.
    missing_sub_ids = aabc_inventory.loc[
        is_register_event(aabc_inventory)
        & (aabc_inventory[study_primary_key_field] == "")
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

    # test subjects that need to be deleted
    test_subjects = aabc_inventory_including_test_subjects.loc[
        aabc_inventory_including_test_subjects[study_primary_key_field].str.contains(
            "test", case=False
        ),
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


code_block_1()


def cron_job_1(qint_df: pd.DataFrame) -> None:
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

    ######
    ###THIS WHOLE SECTION NEEDS TO BE CRON'D - e.g. scan for anything new and import it into Qinteractive - let patch in REDCap handle bad or duplicate data.
    # this is currently taing too much time to iterate through box
    # import anything new by any definition (new name, new sha, new fileid)
    for site_accronym in folder_queue:
        box_folder_id = config["Redcap"]["datasources"]["qint"]["BoxFolders"][
            site_accronym
        ]
        data_access_group_name = config["Redcap"]["datasources"]["aabcarms"][
            site_accronym
        ]["dag"]
        site_number = config["Redcap"]["datasources"]["aabcarms"][site_accronym][
            "sitenum"
        ]

        db = list_files_in_box_folders(box_folder_id)
        db.fileid = db.fileid.astype(int)

        # ones that already exist in q redcap
        cached_filelist = qint_df.copy()
        cached_filelist.fileid = cached_filelist.fileid.astype("Int64")

        # find the new ones that need to be pulled in
        new_file_ids = pd.merge(
            db, cached_filelist.fileid, on="fileid", how="left", indicator=True
        )
        new_file_ids = new_file_ids.loc[new_file_ids._merge == "left_only"].drop(
            columns=["_merge"]
        )
        db2go = db.loc[db.fileid.isin(list(new_file_ids.fileid))]
        if db2go.empty:
            print("NO NEW RECORDS from", site_accronym, "TO ADD AT THIS TIME")
        if not db2go.empty:
            # initiate new ids
            subject_id = cached_filelist.id.astype("Int64").max() + 1
            l = len(db2go)
            vect = []
            for i in range(0, l):
                id = i + subject_id
                vect = vect + [id]

            rows2push = pd.DataFrame(columns=common_form_fields + ravlt_form_fields)
            for i in range(0, db2go.shape[0]):
                redid = vect[i]
                fid = db2go.iloc[i][["fileid"]][0]
                t = box.get_file_by_id(fid)
                created = t.get().created_at
                fname = db2go.iloc[i][["filename"]][0]
                subjid = fname[fname.find("HCA") : 10]
                fsha = db2go.iloc[i][["sha1"]][0]
                print(i)
                print(db2go.iloc[i][["fileid"]][0])
                print(db2go.iloc[i][["filename"]][0])
                print(db2go.iloc[i][["sha1"]][0])
                print("subject id:", subjid)
                print("Redcap id:", redid)
                # pushrow=getrow(fid,fname)
                content = box.read_text(fid)
                assessment = "RAVLT"
                if "RAVLT-Alternate Form C" in content:
                    form = "Form C"
                if "RAVLT-Alternate Form D" in content:
                    form = "Form D"
                if fname.find("Form B") > 0:
                    form = "Form B"
                # visits = sorted(list(map(int,requests.findall('[vV](\d)', fname))))
                a = fname.replace("AV", "").find("V")
                visit = fname[a + 1]
                # visit=visits[-1]
                row = functions.parse_content(content)
                df = pd.DataFrame([row], columns=ravlt_form_fields)
                # print(df)
                firstvars = pd.DataFrame(
                    [
                        [
                            redid,
                            data_access_group_name,
                            site_number,
                            subjid,
                            fid,
                            fname,
                            fsha,
                            created,
                            assessment,
                            visit,
                            form,
                            "",
                            "",
                            "",
                            "",
                        ]
                    ],
                    columns=common_form_fields,
                )
                pushrow = pd.concat([firstvars, df], axis=1)
                rows2push = pd.concat([rows2push, pushrow], axis=0)
                if len(rows2push.subjectid) > 0:
                    print("**************** Summary **********************")
                    print(len(rows2push.subjectid), "rows to push:")
                    print(list(rows2push.subjectid))

            if not rows2push.empty:
                functions.send_frame(
                    dataframe=rows2push,
                    tok=api_key["qint"],
                )


def list_files_in_box_folders(*box_folder_ids) -> pd.DataFrame:
    """List filename, fileid, sha1 for all files in specific box folders

    Args:
        *box_folder_ids: The box id for the folder of interest

    Returns:
        A dataframe with filename, fileid, sha1 for all files in the folder(s)

    """
    filelist = box.list_of_files(box_folder_ids)
    return pd.DataFrame(filelist).transpose()


def code_block_2():
    #########################################################################################
    # PHASE 1 Test that all dataypes expected are present
    # Get the REDCap AABC inventory (which may or may not agree with the reality of data found):
    # there are lots of variables in the inventory.  Don't need them all
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
    qint_report = functions.params_request_report(
        token=api_key["qint"],
        report_id="51037",
    )
    qint_df = functions.get_frame(
        api_url=config["Redcap"]["api_url10"], data=qint_report
    )

    cron_job_1(qint_df)

    # QC checks
    # now check
    qint_df2 = functions.get_frame(
        api_url=config["Redcap"]["api_url10"], data=qint_report
    )
    qint_df2 = qint_df2[["id", "site", "subjectid", "visit"]].copy()
    qint_df2["redcap_event"] = "V" + qint_df2.visit
    qint_df2 = remove_test_subjects(qint_df2, "subjectid")
    # Before merging, check for duplicates that haven't been given the 'unusable' flag
    dups = qint_df.loc[qint_df.duplicated(subset=["subjectid", "visit"])]
    dups2 = dups.loc[~(dups.q_unusable.isnull() == False)]  # or '', not sure

    register_tickets(
        dups2,
        "ORANGE",
        "Duplicate Q-interactive records",
        "AE5001",
    )

    aabc_vs_qint = pd.merge(
        aabc_inventory[keeplist],
        qint_df2.rename(columns={"subjectid": "subject"}).drop(columns=["site"]),
        on=["subject", "redcap_event"],
        how="outer",
        indicator=True,
    )
    aabc_vs_qint["has_qint_data"] = aabc_vs_qint._merge != "left_only"

    qint_only = aabc_vs_qint.loc[aabc_vs_qint._merge == "right_only"]
    register_tickets(
        qint_only[["subject", "redcap_event"]],
        "ORANGE",
        "Subject with Q-int data but ID(s)/Visit(s) are not found in the main AABC-ARMS Redcap.  Please look for typo",
        "AE1001",
    )

    aabc_inventory_plus_qint = aabc_vs_qint.loc[
        aabc_vs_qint._merge != "right_only"
    ].drop(columns=["_merge"])

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

    return aabc_vs_qint, aabc_inventory_plus_qint


aabc_vs_qint, aabc_inventory_plus_qint = code_block_2()


def code_block_3(aabc_vs_qint, aabc_inventory_plus_qint):
    # NOW FOR TOOLBOX. ############################################################################
    # # 1. grab partial files from intraDB
    # # 2. QC (after incorporating patches)
    # # 3. generate tickets and send to JIra if don't already exist
    # # 4. send tickets that arent identical to ones already in Jira
    # # 5. concatenate legit data (A scores file and a Raw file, no test subjects or identical duplicates -- no 'Narrow' or 'Registration' datasets)
    # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

    ##FIRST THE RAW DATA FILES
    rawd4 = cat_toolbox_rawdata_files("AABC_WU_ITK")
    rawd1 = cat_toolbox_rawdata_files("AABC_MGH_ITK")
    rawd3 = cat_toolbox_rawdata_files("AABC_UMN_ITK")
    rawd2 = cat_toolbox_rawdata_files("AABC_UCLA_ITK")
    # note that some of these won't work because UCLA hasn't started collecting data
    raw41 = functions.TLBXreshape(rawd4)
    raw11 = functions.TLBXreshape(rawd1)
    raw31 = functions.TLBXreshape(rawd3)
    raw21 = functions.TLBXreshape(rawd2)

    # remove files known to be duds.
    rf2 = pd.concat([raw41, raw31, raw21, raw11])
    rf2 = remove_test_subjects(rf2, "PIN")
    rf2 = rf2.loc[~(rf2.PIN.str.upper() == "ABC123")]

    # drop duplicates for purpose of generating QC flags.
    rf2 = rf2.drop_duplicates(subset="PIN").copy()

    # fixtypos - NEED TO incorporate information about date of session as given in filename because of typos involving legit ids
    # THERE IS A SUBJECT HERE WHOSE NEXT VISIT WILL BE IN CONFLICT WITH THIS ONE, OTHERWISE
    fix_typos = aabc_vs_qint.loc[aabc_vs_qint.nih_toolbox_upload_typo != ""][
        ["subject", "redcap_event", "nih_toolbox_upload_typo"]
    ]
    fix_typos["PIN"] = fix_typos.subject + "_" + fix_typos.redcap_event
    fixes = dict(zip(fix_typos.nih_toolbox_upload_typo, fix_typos.PIN))
    rf2.PIN = rf2.PIN.replace(fixes)

    # NOW THE SCORED DATA
    results4 = cat_toolbox_score_files("AABC_WU_ITK")
    results1 = cat_toolbox_score_files("AABC_MGH_ITK")
    results3 = cat_toolbox_score_files("AABC_UMN_ITK")
    results2 = cat_toolbox_score_files("AABC_UCLA_ITK")

    # THERE IS A SUBJECT HERE WHOSE NEXT VISIT WILL BE IN CONFLICT WITH THIS ONE HCA8596099_V3...FIX before 2023
    # still not sure how to get filename next to the contents of the file, given the fact that there are spaces in the name. Bleh
    # this is close, but wont work for case of multipe PINs in a single file
    # find /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data -type f -name "*Score*" -print0 | while IFS= read -r -d '' file; do echo "${file}," && head -2 "$file" | tail -1; done
    # cat /ceph/intradb/archive/AABC_WU_ITK/resources/toolbox_endpoint_data/"2022-09-07 10.04.20 Assessment Scores.csv_10.27.127.241_2022-09-07T10:04:36.2-05:00_olivera" | grep HCA8596099_V3 | sed 's/HCA8596099_V3/HCA8596099_V2/g'

    # note that some of these won't work because UCLA hasn't started collecting data
    dffull1 = functions.TLBXreshape(results1)
    dffull2 = functions.TLBXreshape(results2)
    dffull3 = functions.TLBXreshape(results3)
    dffull4 = functions.TLBXreshape(results4)

    ##fixtypos in Scores file now
    dffull = pd.concat([dffull1, dffull3, dffull2, dffull4])
    dffull = dffull.copy()  # pd.concat([df11,df31])
    dffull = remove_test_subjects(dffull, "PIN")
    dffull = dffull.loc[~(dffull.PIN.str.upper() == "ABC123")]

    dffull.PIN = dffull.PIN.replace(fixes)

    # merge with patch fixes (i.e. delete duplicate specified in visit summary)
    # This is a single fix... need to generalized to all instruments and their corresponding dupvars:
    # -->HCA8596099_V3 has 2 assessments for Words in Noise - add patch note"
    instrument = "NIH Toolbox Words-In-Noise Test Age 6+ v2.1"
    dupvar = "tlbxwin_dups_v2"
    iset = aabc_vs_qint
    dffull = functions.filterdupass(instrument, dupvar, iset, dffull)

    # find any non-identical duplicated Assessments still in data after patch
    dupass = dffull.loc[dffull.duplicated(subset=["PIN", "Inst"], keep=False)][
        ["PIN", "Assessment Name", "Inst"]
    ]
    dupass = dupass.loc[~(dupass.Inst.str.upper().str.contains("ASSESSMENT"))]

    # TURN THIS INTO A TICKET

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

    # DATE FORMAT IS STILL FUNKY ON THIS CHECK, better to examine by hand until can figure out why str.split isn't working.
    # identical dups are removed if they have identical dates in original ssh command.  These will catch leftovers
    # find cases where PIN was reused (e.g. PIN is the same but date more than 3 weeks different
    dffull["Date"] = dffull.DateFinished.str.split(" ", expand=True)[0]

    # add subject and visit
    df2 = dffull.drop_duplicates(subset="PIN").copy()
    df2["redcap_event"] = df2.PIN.str.split("_", expand=True)[1]
    df2["subject"] = df2.PIN.str.split("_", expand=True)[0]
    df2["redcap_event"] = df2.PIN.str.split("_", expand=True)[1]

    # now merge with inventory
    aabc_inventory_5 = pd.merge(
        aabc_inventory_plus_qint,
        df2[["subject", "redcap_event", "PIN"]],
        on=["subject", "redcap_event"],
        how="outer",
        indicator=True,
    )
    aabc_inventory_5["has_tlxb_data"] = aabc_inventory_5._merge != "left_only"

    # find toolbox records that aren't in AABC - typos are one thing...legit ids are bad because don't know which one is right unless look at date, which is missing for cog comps
    # turn this into a ticket
    t2 = aabc_inventory_5.loc[
        aabc_inventory_5._merge == "right_only", ["PIN", "subject", "redcap_event"]
    ]
    register_tickets(
        t2,
        "ORANGE",
        "TOOLBOX PINs are not found in the main AABC-ARMS Redcap.  Typo?",
        "AE1001",
    )

    aabc_inventory_5 = aabc_inventory_5.loc[
        aabc_inventory_5._merge != "right_only"
    ].drop(columns=["_merge"])

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
    # TODO: Make sure these columns are available in dataframes sent to the ticketing system
    # "subject",
    # "study_id",
    # "redcap_event_name",
    # "redcap_event",
    # "event_date",
    # "PIN",
    # "reason",
    # "code",
    register_tickets(
        t3,
        "ORANGE",
        "Missing TLBX data",
        "AE2001",
    )

    return aabc_inventory_5


aabc_inventory_5 = code_block_3(aabc_vs_qint, aabc_inventory_plus_qint)


def code_block_4(aabc_inventory_5):
    ### NOW For ASA 24 ######################################################################
    # ORDER
    # 1. scan for data (here just looking for existende)
    # 2. QC ids (after incorporating patches and translating ASAID into AABC id)
    # 3. generate tickets and send to JIra if don't already exist
    #  4. send tickets that arent identical to ones already in Jira
    # # # 5. just dump all legit data to BOX (transform to be defined later) after patching, dropping restricted variables, and merging in subject and redcap_event
    # # # 6. create and send snapshot of patched data to BOX after dropping restricted variables

    folder_queue = ["WU", "UMN", "MGH"]  # UCLA and MGH not started yet
    anydata = set()
    for site_accronym in folder_queue:
        box_folder_id = config["NonQBox"]["ASA24"][site_accronym]
        dbitems = list_files_in_box_folders(box_folder_id)
        for f in dbitems.fileid:
            print(f)
            k = box.read_csv(f)
            anydata.update(k.UserName)

    AD = pd.DataFrame(anydata, columns=["asa24id"])
    aabc_inventory_6 = pd.merge(aabc_inventory_5, AD, on="asa24id", how="left")
    aabc_inventory_6["has_asa24_data"] = aabc_inventory_6._merge != "left_only"
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
    return aabc_inventory_6


aabc_inventory_6 = code_block_4(aabc_inventory_5)


def code_block_5(aabc_inventory_6):
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
        actsubs = []
        for fid in dbitems.fileid:
            try:
                patrn = "Identity"
                f = box.download_file(fid, download_dir="tmp", override_if_exists=False)
                print(f)
                file_one = open(f, "r")
                variable = file_one.readline(1)
                if not variable == "":
                    for l in file_one.readlines():
                        if re.search(patrn, l):
                            hcaid = ""
                            hcaid = l.strip("\n").replace('"', "").split(",")[1]
                            print("Inner", f, "has", hcaid)
                            actsubs = actsubs + [hcaid]
                file_one.close()
            except:
                print("Something the matter with file", f)
        actdata = actdata + list(actsubs)  # list(set(actsubs))

    # Duplicates?
    duplicated_actigraphy_records = [
        item for item, count in collections.Counter(actdata).items() if count > 1
    ]
    if duplicated_actigraphy_records != "":
        print(
            "Duplicated Actigraphy Record Found:",
            duplicated_actigraphy_records,
        )

    ActD = pd.DataFrame(actdata, columns=["PIN"])
    inventoryaabc6 = pd.merge(
        aabc_inventory_6, ActD, on="PIN", how="left", indicator=True
    )
    inventoryaabc6["has_actigraphy_data"] = inventoryaabc6._merge != "left_only"

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

    return inventoryaabc6


inventoryaabc6 = code_block_5(aabc_inventory_6)


def code_block_6(inventoryaabc6):
    # MOCA SPANISH  #############################################################
    ## no data yet

    ############################################################################
    # Psychopy
    # ORDER
    # 1. scan for data in Box (here just looking for existence) and in IntraDB
    # 2. QC ids (after incorporating patches)
    # 3. generate an tickets and send to JIra if don't already exist
    # 4. DONT dump or snapshot.  Leave data in IntraDB.

    site_accronym = "WU"
    folder_queue = ["WU", "MGH", "UMN"]  # UCLE

    # scan Box
    anydata = pd.DataFrame()
    for site_accronym in folder_queue:
        box_folder_id = config["NonQBox"]["Psychopy"][site_accronym]
        dbitems = list_files_in_box_folders(box_folder_id)
        for fname in dbitems.filename:
            subjvscan = fname[fname.find("HCA") : fname.find("HCA") + 15]
            l2 = subjvscan.split("_")
            row = l2 + [fname]
            print(row)
            rowfor = pd.DataFrame(row).transpose()
            anydata = pd.concat([anydata, rowfor])

    anydata.columns = ["subject", "redcap_event", "scan", "fname"]
    checkIDB = anydata[["subject", "redcap_event", "scan"]]
    checkIDB["PIN_AB"] = (
        checkIDB.subject + "_" + checkIDB.redcap_event + "_" + checkIDB.scan
    )
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
        inventoryaabc6.loc[inventoryaabc6.redcap_event.str.contains("V")].drop(
            columns=["subject_id", "subject"]
        ),
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
    inventoryaabc7 = pd.merge(
        inventoryaabc6, PSY2, on=["subject", "redcap_event"], how="left", indicator=True
    )
    inventoryaabc7["has_psychopy_data"] = inventoryaabc7._merge != "left_only"
    missingPY = inventoryaabc7.loc[
        inventoryaabc7.redcap_event_name.str.contains("v")
        & ~inventoryaabc7.has_psychopy_data,
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
    register_tickets(
        missingPY,
        "ORANGE",
        "PsychoPy cannot be found in BOX or IntraDB",
        "AE4001",
    )
    return inventoryaabc7


inventoryaabc7 = code_block_6(inventoryaabc6)


def code_block_7(inventoryaabc7):

    ##################################################################################
    # HOT FLASH DATA (not available yet)

    ###################################################################################
    # VNS (not available yet)

    ###################################################################################
    # To DO: Forgot to CHECK FOR BUNK IDS IN PSYCHOPY AND ACTIGRAPHY
    ###################################################################################

    ###################################################################################
    # NOW CHECK key REDCap AABC variables for completeness (counterbalance, inventory completeness, age, bmi and soon to be more)
    # inventory_complete
    pd.set_option("display.width", 1000)
    pd.options.display.width = 1000

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

    summv = inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains("v")][
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


code_block_7(inventoryaabc7)


def code_block_8(inventoryaabc7):

    agev = inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains("v")][
        [
            "redcap_event",
            "study_id",
            "site",
            "subject",
            "redcap_event_name",
            "age_visit",
            "event_date",
            "v0_date",
        ]
    ]
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
    register_tickets(
        agemv, "RED", "Age outlier. Please double check DOB and Event Date", "AE7001"
    )

    ageav = agev.loc[
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
    register_tickets(
        ageav, "RED", "Missing Age. Please check DOB and Event Date", "AE3001"
    )


code_block_8(inventoryaabc7)


def code_block_9(inventoryaabc7):

    # calculate BMI: weight (lb) / [height (in)]2 x 703
    # inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains('v')][['subject','redcap_event_name','height_ft','height_in','weight','bmi','event_date']]
    bmiv = inventoryaabc7.loc[inventoryaabc7.redcap_event_name.str.contains("v")][
        ["bmi", "redcap_event", "subject", "study_id", "site", "event_date"]
    ].copy()
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
    register_tickets(
        a, "RED", "BMI is an outlier.  Please double check height and weight", "AE7001"
    )

    # missings
    bmiv = bmiv.loc[
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
        bmiv,
        "RED",
        "Missing Height or Weight (or there is another typo preventing BMI calculation)",
        "AE3001",
    )


code_block_9(inventoryaabc7)


def combine_tickets_into_jira(
    Q1,
    Q2,
    a1,
    a2,
    P,
    C,
    summv,
    agemv,
    ageav,
    a,
    bmiv,
    T,
    inventoryaabc7,
    inventoryaabc6,
):
    ##############################################################################
    # all the flags for JIRA together
    QAAP = functions.concat(Q1, Q2, a1, a2, P, C, summv, agemv, ageav, a, bmiv, T)
    QAAP["QCdate"] = date.today().strftime("%Y-%m-%d")
    QAAP["issue_age"] = pd.to_datetime(QAAP.QCdate) - pd.to_datetime(QAAP.event_date)
    QAAP = QAAP[
        [
            "subject",
            "redcap_event",
            "study_id",
            "site",
            "reason",
            "code",
            "event_date",
            "issue_age",
        ]
    ]
    QAAP.sort_values(["site", "issue_age"], ascending=False).to_csv("test.csv")

    ##REDUCE by Color code.... need to be able to change these values.
    filteredQ = QAAP.loc[
        ((QAAP.code == "RED") & (QAAP.issue_age.dt.days > 7))
        | ((QAAP.code == "ORANGE") & (QAAP.issue_age.dt.days > 18))
        | ((QAAP.code == "GREEN") & (QAAP.issue_age.dt.days > 28))
        | ((QAAP.code == "YELLOW") & (QAAP.issue_age.dt.days > 35))
    ]
    # RED=issue_age>7
    # ORANGE=issues_age>18
    # YELLOW=issue_age>28
    # GREEN=issue_age>35

    ####### Download existing JIRA tickets and reduce QAAP accordingly
    ## create and upload new tickets.
    #
    ########################################################################
    # clean up the AABC inventory and upload to BOX for recruitment stats.
    inventoryaabc7.loc[inventoryaabc7.age == "", "age"] = inventoryaabc6.age_visit
    inventoryaabc7.loc[
        inventoryaabc7.event_date == "", "event_date"
    ] = inventoryaabc7.v0_date
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


combine_tickets_into_jira(
    Q1,
    Q2,
    a1,
    a2,
    P,
    C,
    summv,
    agemv,
    ageav,
    a,
    bmiv,
    T,
    inventoryaabc7,
    inventoryaabc6,
)
