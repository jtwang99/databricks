# Databricks notebook source
import json
import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
context_info = json.loads(ctx.toJson())
print(json.dumps(context_info, indent=2))

# COMMAND ----------

# DBTITLE 1,get username
user_name = ctx.tags().get("user").get()
print(user_name)

# COMMAND ----------

# DBTITLE 1,get domain
DOMAIN = ctx.tags().get("browserHostName").get()
print("DOMAIN = {}".format(DOMAIN))

# COMMAND ----------

# DBTITLE 1,get ad-hoc Token
TOKEN = ctx.apiToken().get()
print("TOKEN = {}".format(TOKEN))

# COMMAND ----------

# DBTITLE 1,get notebook path
notebook_path = ctx.extraContext().get("notebook_path").get()
print(notebook_path)
