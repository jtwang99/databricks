# Databricks notebook source
# MAGIC %sh ls -l /databricks-datasets

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %sql CREATE TABLE default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')

# COMMAND ----------

# MAGIC %sql select * from default.people10m

# COMMAND ----------

import requests
import json
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host_name = ctx.tags().get("browserHostName").get()
ad_hoc_token = ctx.apiToken().get()
print("Domain = {}".format(host_name))
print("Token  = {}".format(ad_hoc_token))

# COMMAND ----------

cluster_id = ctx.tags().get("clusterId").get()
response = requests.get(
    f'https://{host_name}/api/2.0/clusters/get?cluster_id={cluster_id}',
    headers={'Authorization': f'Bearer {ad_hoc_token}'}
  ).json()
print(f"driver type={response['driver_node_type_id']} private_ip={response['driver']['private_ip']} host_private_ip={response['driver']['host_private_ip']}")
print(f"worker type={response['node_type_id']}")

# COMMAND ----------

DOMAIN=host_name
TOKEN=ad_hoc_token

# COMMAND ----------

def SubmitDatabricksRun(notebook_name):
  json_str="""
    "existing_cluster_id": "0906-031925-pond865",
    "notebook_path": "{}"
  """.format(notebook_name)
  json_str = "{}{}{}".format('{',json_str,'}')
  print(json_str)

  response = requests.post(
    'https://%s/api/2.0/jobs/runs/submit' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# COMMAND ----------

SubmitDatabricksRun('/Users/jt.wang@auo.com/test')

# COMMAND ----------

def ListDatabricksJob():
  response = requests.get(
  'https://%s/api/2.0/jobs/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN}
  )
  
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
      print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return


# COMMAND ----------

ListDatabricksJob()

# COMMAND ----------

def ListDatabricksCluster():
  response = requests.get(
    'https://%s/api/2.0/clusters/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN}
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# COMMAND ----------

ListDatabricksCluster()

# COMMAND ----------

def GetDatabricksCluster(cluster_id):
  json_str="""
    "cluster_id": "{}"
  """.format(cluster_id)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.get(
    'https://%s/api/2.0/clusters/get' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# COMMAND ----------

GetDatabricksCluster('0903-022151-fell921')

# COMMAND ----------

import requests
from requests_negotiate_sspi import HttpNegotiateAuth
import json
import keyring

proxy_host = "auhqproxy.corpnet.auo.com"
proxy_port = "8080"
proxies = {
       "http": "http://{}:{}/".format( proxy_host, proxy_port),
       "https": "http://{}:{}/".format( proxy_host, proxy_port)
}
'''
DOMAIN = 'adb-8707839132747967.7.azuredatabricks.net' #new lcd1databricks
TOKEN = keyring.get_password("jtwang-databricks-token", "token")
'''
DOMAIN = 'adb-6150091341606359.19.azuredatabricks.net' #new lcd2databricks
TOKEN ='dapidec605c9847dcaf96684f968518a87f2'

# UDF ----------
def CreateDatabricksSecretScope(scope_name):
  json_str="""
    "scope": "{}"
  """.format(scope_name)
  json_str = "{}{}{}".format('{',json_str,'}')
  #print(json_str)

  response = requests.post(
    'https://%s/api/2.0/secrets/scopes/create' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# UDF ----------
def ListDatabricksSecretScope():
  response = requests.get(
  'https://%s/api/2.0/secrets/scopes/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  proxies=proxies,  auth=HttpNegotiateAuth()
  )
  
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
      print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# UDF ----------
def PutSecretToDatabricks(scope, key, string_value):
  json_str="""
    "scope": "{}",
    "key": "{}",
    "string_value": "{}"
  """.format(scope, key, string_value)
  json_str = "{}{}{}".format('{',json_str,'}')
  #print(json_str)

  response = requests.post(
    'https://%s/api/2.0/secrets/put' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )

  if response.status_code != 200:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return



# UDF ----------
def ListDatabricksSecret(scope):
  json_str="""
    "scope": "{}"
  """.format(scope)
  json_str = "{}{}{}".format('{',json_str,'}')
  #print(json_str)

  response = requests.get(
    'https://%s/api/2.0/secrets/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# UDF ----------
def PutSecretACLToDatabricks(scope, principal, permission):
  json_str="""
    "scope": "{}",
    "principal": "{}",
    "permission": "{}"
  """.format(scope, principal, permission)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.post(
    'https://%s/api/2.0/secrets/acls/put' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )

  if response.status_code != 200:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return


# UDF ----------
def ListDatabricksSecretACL(scope_name):
  json_str="""
    "scope": "{}"
  """.format(scope_name)
  json_str = "{}{}{}".format('{',json_str,'}')

  s = requests.Session()
  s.verify=False
  s.proxies=proxies
  s.auth=HttpNegotiateAuth()
  
  response = s.get(
    'https://%s/api/2.0/secrets/acls/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# UDF ----------
def ListDatabricksToken():
  response = requests.get(
  'https://%s/api/2.0/token/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  proxies=proxies,  auth=HttpNegotiateAuth()
  )
  
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
      print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

# UDF ----------
def ListDatabricksJob():
  response = requests.get(
  'https://%s/api/2.0/jobs/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  proxies=proxies,  auth=HttpNegotiateAuth()
  )
  
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
      print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

def CancelDatabricksJob(run_id):
  json_str="""
    "run_id": "{}"
  """.format(job_id)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.post(
    'https://%s/api/2.0/jobs/runs/cancel' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )

def GetDatabricksJob(job_id):
  json_str="""
    "job_id": "{}"
  """.format(job_id)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.get(
    'https://%s/api/2.0/jobs/get' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return



def GetDatabricksJobRun(run_id):
  json_str="""
    "run_id": "{}"
  """.format(run_id)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.get(
    'https://%s/api/2.0/jobs/runs/get-output' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return response

def ListDatabricksCluster():
  response = requests.get(
    'https://%s/api/2.0/clusters/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth()
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return


def GetDatabricksCluster(cluster_id):
  json_str="""
    "cluster_id": "{}"
  """.format(cluster_id)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.get(
    'https://%s/api/2.0/clusters/get' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

def ListDatabricksClusterPool():
  response = requests.get(
    'https://%s/api/2.0/instance-pools/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth()
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

def  UpdateDatabricksClusterPool(instance_pool_id, instance_pool_name, min_idle_instances, idle_instance_autotermination_minutes):
  json_str="""
    "instance_pool_id": "{}",
    "instance_pool_name": "{}",
    "min_idle_instances": "{}",
    "idle_instance_autotermination_minutes": "{}"
  """.format(instance_pool_id, instance_pool_name, min_idle_instances, idle_instance_autotermination_minutes)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.post(
    'https://%s/api/2.0/instance-pools/edit' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )
  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return

def RunDatabricksJob(job_id, params):
  json_str="""
    "job_id": {},
     "notebook_params": {}
  """.format(job_id, params)
  json_str = "{}{}{}".format('{',json_str,'}')

  response = requests.post(
    'https://%s/api/2.0/jobs/run-now' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    proxies=proxies,  auth=HttpNegotiateAuth(),
    json=json.loads(json_str)
  )

  if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
    run_id = response.json()["run_id"]
  else:
    print("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  return run_id
#-----------------------------
'''
ListDatabricksToken()
ListDatabricksSecretScope()
ListDatabricksJob()
'''
import time, polling2

def CheckJobRunStatus(response):
    """Check that the response returned 'success'"""
    return response.json()["metadata"][ "state"]["life_cycle_state"] == 'TERMINATED'

'''
run_id = RunDatabricksJob('118', '{ "X": "john doe",  "Y": "35" }')

polling2.poll(
    lambda: GetDatabricksJobRun(run_id),
    check_success=CheckJobRunStatus,
    step=5,
    timeout=100)

time.sleep(8)
'''
#response=GetDatabricksJobRun('6280')
#print(json.dumps(response.json(), indent=2))
#print("execution_duration(ms) = {}".format(response.json()["metadata"]["execution_duration"]))
#print("result = {}".format(response.json()["notebook_output"]["result"]["filename"]))

#ListDatabricksCluster()
#GetDatabricksCluster('0302-072724-baas215')
#GetDatabricksCluster('0226-024100-coven594')
#GetDatabricksJob('26')
#ListDatabricksClusterPool()
#UpdateDatabricksClusterPool('0514-081656-forms793-pool-MlyBZYOf', '8G', '1', '10')
#UpdateDatabricksClusterPool('0514-081656-forms793-pool-MlyBZYOf', '8G', '0', '1')
#PutSecretACLToDatabricks('tableau-secret-scope', 'users', 'READ')
#ListDatabricksSecretACL('tableau-secret-scope')
#PutSecretACLToDatabricks('synapse-db-scope', 'users', 'READ')
#PutSecretACLToDatabricks('synapse-db-scope', 'admins', 'MANAGE')
requests.packages.urllib3.disable_warnings()
ListDatabricksSecretACL('synapse-db-scope')

