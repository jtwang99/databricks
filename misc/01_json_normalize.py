# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://colab.research.google.com/gist/dmort-ca/73719647d2fbe50cb0c695d38e8d5ee6/01_json_normalize.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

# COMMAND ----------

# MAGIC %pip install -q atlassian-python-api

# COMMAND ----------

from atlassian import Jira
from IPython.display import display, HTML, display_html
import pandas as pd

pd.set_option('max_colwidth', 140)

# COMMAND ----------

jira = Jira(
    url = "",
    username = "",
    password = "",
)

# COMMAND ----------

results = jira.jql("project = CAE", limit=5, fields=["issuetype", "status", "status", "summary"])

# COMMAND ----------

json_str ="""
{
    "id": "0001",
    "type": "donut",
    "name": "Cake",
    "ppu": 0.55,
    "batters":
        {
            "batter":
                [
                    { "id": "1001", "type": "Regular" },
                    { "id": "1002", "type": "Chocolate" },
                    { "id": "1003", "type": "Blueberry" },
                    { "id": "1004", "type": "Devil's Food" }
                ]
        },
    "topping":
        [
            { "id": "5001", "type": "None" },
            { "id": "5002", "type": "Glazed" },
            { "id": "5005", "type": "Sugar" },
            { "id": "5007", "type": "Powdered Sugar" },
            { "id": "5006", "type": "Chocolate with Sprinkles" },
            { "id": "5003", "type": "Chocolate" },
            { "id": "5004", "type": "Maple" }
        ]
}
"""

# COMMAND ----------

results=json.loads(json_str)

# COMMAND ----------

import json
json_str = """
{
  "expand": "schema,names",
  "issues": [
    {
      "fields": {
        "issuetype": {
          "avatarId": 10300,
          "description": "",
          "id": "10005",
          "name": "New Feature",
          "subtask": False
        },
        "status": {
          "description": "A resolution has been taken, and it is awaiting verification by reporter. From here issues are either reopened, or are closed.",
          "id": "5",
          "name": "Resolved",
          "statusCategory": {
            "colorName": "green",
            "id": 3,
            "key": "done",
            "name": "Done",
          }
        },
        "summary": "Recovered data collection Defraglar $MFT problem"
      },
      "id": "11861",
      "key": "CAE-160",
    },
      "fields": { 
      "maxResults": 5,
      "startAt": 0,
      "total": 160
    }]
}
"""
results=json.loads(json_str)

# COMMAND ----------

results_str

# COMMAND ----------

results_str=json.loads(results_str)
#df=pd.read_json(results_str)


# COMMAND ----------

df = pd.DataFrame.from_records(results["issues"], columns=["key", "fields",])

# COMMAND ----------

df = pd.DataFrame.from_records(results["issues"], columns=["key", "fields",])

df = (
    df["fields"]
    .apply(pd.Series)
    .merge(df, left_index=True, right_index = True)
)
df.drop(columns = "fields", inplace = True)

# COMMAND ----------

# Extract the issue type name and assign it to a new column called "issue_type"
df_issue_type = (
    df["issuetype"]
    .apply(pd.Series)
    .rename(columns={"name": "issue_type_name"})["issue_type_name"]
)
df = df.assign(issue_type_name = df_issue_type)
df.drop(columns = "issuetype", inplace = True)
df

# COMMAND ----------

FIELDS = ["key", "fields.summary", "fields.issuetype.name", "fields.status.name", "fields.status.statusCategory.name"]
df = pd.json_normalize(results["issues"])
df[FIELDS]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Some Extras

# COMMAND ----------

# Use record_path instead of passing the list contained in results["issues"]
pd.json_normalize(results, record_path="issues")[FIELDS]

# COMMAND ----------

# Only recurse down to the second level, in this case the statusCategory name field won't be included in the resulting DataFrame.
pd.json_normalize(results, record_path="issues", max_level = 2)[FIELDS]

# COMMAND ----------

# Separate level prefixes with a "-" instead of the default "."
FIELDS = ["key", "fields-summary", "fields-issuetype-name", "fields-status-name", "fields-status-statusCategory-name"]
pd.json_normalize(results["issues"], sep = "-")[FIELDS]
