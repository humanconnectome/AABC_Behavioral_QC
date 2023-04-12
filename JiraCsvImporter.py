#!/usr/bin/env python3
"""
This CLI utility import CSV files into Jira tickets.
"""

import click
import pandas as pd
from ccf.config import LoadSettings

from creds import *
from jira import JIRA, Issue
from JiraConverter import LIST_JIRA_FIELDS_IN_ORDER, read_csv_cols

config = LoadSettings()
secret = pd.read_csv(config["config_files"]["secrets"])


LIST_VALID_COMPONENTS = [
    "Actigraphy",
    "ASA24",
    "PsychoPy",
    "RAVLT",
    "REDCap",
    "TLBX",
    "HotFlash"
]

# Custom Fields for Jira
MAP_FIELD_TO_N = {
    "error_code": 10800,
    "event_n": 10801,
    "event_date": 10802,
}


def as_search_field(field):
    if field not in MAP_FIELD_TO_N:
        raise ValueError(f"Invalid field: {field}")
    field_n = MAP_FIELD_TO_N[field]
    return f"cf[{field_n}]"


def as_field(field):
    if field not in MAP_FIELD_TO_N:
        raise ValueError(f"Invalid field: {field}")
    field_n = MAP_FIELD_TO_N[field]
    return f"customfield_{field_n}"


def create_jira_ticket(row: pd.Series) -> Issue:
    """Create a Jira ticket from a row in the DataFrame

    Args:
            row: row from DataFrame

    Returns:
            Jira ticket

    Side Effects:
            Creates a Jira ticket on the server
    """
    issue_dict = {
        "project": row.project,
        # "project": "AABCBLANK",
        "summary": row.summary,
        "description": row.description,
        "issuetype": {"name": "Task"},
    }
    if pd.notna(row.error_code):
        # Must be list of strings
        issue_dict[as_field("error_code")] = row.error_code.split(",")

    if pd.notna(row.event_n):
        # Must be list of strings
        issue_dict[as_field("event_n")] = row.event_n.split(",")

    if pd.notna(row.event_date):
        # TODO: Enforce format of YYYY-MM-DD
        issue_dict[as_field("event_date")] = row.event_date

    if pd.notna(row.component):
        components = set(row.component.split(","))
        invalid_components = components - set(LIST_VALID_COMPONENTS)
        if len(invalid_components) > 0:
            raise ValueError(
                f"Invalid components: {invalid_components} for `{row.summary}`"
            )

        issue_dict["components"] = [{"name": c} for c in components]

    return jira.create_issue(fields=issue_dict)


def search_for_ticket(row: pd.Series) -> str:
    """Search for existing Jira ticket with the same summary and error code

    Args:
        row: row from DataFrame

    Returns:
        Jira ticket key if found, else empty string
    """
    jql_search_query = f"summary ~ '{row.summary}'"

    # Search for tickets with same Error Codes
    if pd.notna(row.error_code):
        error_codes = row.error_code.split(",")
        for code in error_codes:
            jql_search_query += f" AND {as_search_field('error_code')} = '{code}'"

    issues = jira.search_issues(jql_search_query)
    issue_keys = ",".join([i.key for i in issues])
    return issue_keys


def create_if_not_exists(row: pd.Series) -> str:
    """
    Create Jira ticket if it does not already exist. It first does a search.
    Args:
        row: Tickt row from DataFrame

    Returns:
        Jira ticket key

    Side Effects:
        Prints to stdout whether ticket was created or not
    """
    issues = search_for_ticket(row)
    if issues:
        print(f"Ticket(s) already exists: {issues}")
        #TO DO : split out site string
        #print(f"Ticket(s) already exists: https://issues.humanconnectome.org/projects/AABC*/issues/{issues}")
    else:
        if DRYRUN:
            print(f"DRYRUN: Would create ticket: {row.summary}")
        else:
            result = create_jira_ticket(row)
            issues = result.key
            print(f"Created new ticket: {issues}")
    return issues


DRYRUN = False
jira = None
BOT_PERSONAL_ACCESS_TOKEN = (
    secret.loc[secret.source == "JIRA_PERSONAL_ACCESS_TOKEN", "api_key"]
    .reset_index()
    .drop(columns="index")
    .api_key[0]
)
API_URL = (
    secret.loc[secret.source == "JIRA_API_URL", "api_key"]
    .reset_index()
    .drop(columns="index")
    .api_key[0]
)


@click.command("cli", context_settings={"show_default": True}, help=__doc__)
@click.argument("csv_file", type=click.Path(exists=True), default="./jira_tickets.csv")
@click.option("-n", "--dry-run", is_flag=True, help="Do NOT create actual tickets")
def main(csv_file, dry_run=False):
    global DRYRUN, jira
    DRYRUN = dry_run
    jira = JIRA(API_URL, token_auth=BOT_PERSONAL_ACCESS_TOKEN)
    df = read_csv_cols(csv_file, LIST_JIRA_FIELDS_IN_ORDER)
    results = df.apply(create_if_not_exists, axis=1)

    # TODO: Do something with results???


if __name__ == "__main__":
    main()
