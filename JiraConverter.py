#!/usr/bin/env python3
"""
This CLI utility converts a CSV file from Petra's error format into a format
that can be imported into Jira, using companion tool `JiraCsvImporter`
"""

import pandas as pd
import click


# Lookup Maps
MAP_SITE_TO_PROJECT_ACRONYM = {
    1: "AABCMGH",
    2: "AABCUCLA",
    3: "AABCUMN",
    4: "AABCWU",
    # (default value) NA values will be filled with 0, before replacement:
    0: "AABCWU",
}
MAP_COLOR_TO_MIN_DAYS = dict(
    RED=7,
    ORANGE=18,
    YELLOW=28,
    GREEN=35,
    # Anything not listed above will be treated as "DEFAULT", including NA values
    DEFAULT=0,
)

LIST_JIRA_FIELDS_IN_ORDER = [
    "project",
    "summary",
    "description",
    "error_code",
    "event_n",
    "event_date",
    "component",
]


def ticket_old_enough(df_row: pd.Series) -> bool:
    """Return True if the ticket row is old enough to be created in Jira"""
    color = df_row.code if df_row.code in MAP_COLOR_TO_MIN_DAYS else "DEFAULT"
    min_days = MAP_COLOR_TO_MIN_DAYS[color]
    return df_row.issue_age >= min_days


def read_csv_cols(csv_path: str, cols: set) -> pd.DataFrame:
    """Read the CSV file into a DataFrame and make sure expected columns exist

    Args:
        csv_path: Path to the CSV file
        cols: Set of column names to check for (all other columns will be dropped)

    Returns:
        DataFrame with only the columns in `cols`
    """
    df = pd.read_csv(csv_path)
    actual = set(df.columns)

    if not actual.issuperset(cols):
        raise ValueError(f"File: {csv_path} is missing columns: {cols - actual}")

    return df[list(cols)].copy()


@click.command("cli", context_settings={"show_default": True}, help=__doc__)
@click.argument("csv_file", type=click.Path(exists=True))
@click.option(
    "--output",
    "-o",
    type=click.Path(writable=True),
    default="./jira_tickets.csv",
    help="CSV file to write Jira Tickets to",
)
@click.option(
    "--show-dropped", "-v", is_flag=True, help="Show dropped tickets (`dropped.csv`)"
)
def to_jira_df(
    csv_file: str,
    output: str = "./jira_tickets.csv",
    show_dropped: bool = False,
):
    df = read_csv_cols(
        csv_file,
        {
            "site",
            "code",
            "issue_age",
            "subject",
            "issueCode",
            "redcap_event",
            "event_date",
            "datatype",
            "reason",
            "study_id",
        },
    )

    # Give Jira specific names
    df = df.rename(
        columns={
            "issueCode": "error_code",
            "redcap_event": "event_n",
            "datatype": "component",
        }
    )

    df["project"] = df.site.fillna(0).astype(int).replace(MAP_SITE_TO_PROJECT_ACRONYM)
    df["summary"] = df["subject"] + "." + df["event_n"] + " - " + df["component"]
    df["description"] = (
        df["reason"]
        + "\nevent_date: "
        + df["event_date"]
        + "\nstudy_id: "
        + df["study_id"]
    )

    # Convert to int, before => after: `21 days` => `21`, `0 days` => `0`, `nan` => `0`
    df.issue_age = (
        df.issue_age.str.extract(r"(\d+) days", expand=False).fillna(0).astype(int)
    )

    is_old_enough = df.apply(ticket_old_enough, axis=1)

    # These tickets are too young/soon to create Jira tickets for
    dropped_tickets = df[~is_old_enough].copy()
    if show_dropped:
        dropped_tickets.to_csv("dropped.csv", index=False)

    # These tickets are old enough
    to_jira = df[is_old_enough].copy()

    # Re-order the fields
    to_jira = to_jira[LIST_JIRA_FIELDS_IN_ORDER]
    to_jira.to_csv(output, index=False)


if __name__ == "__main__":
    to_jira_df()
