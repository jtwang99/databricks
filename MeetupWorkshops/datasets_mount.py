# Databricks notebook source
# MAGIC %python
# MAGIC ACCESSY_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
# MAGIC SECERET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
# MAGIC 
# MAGIC mounts_list = [
# MAGIC {'bucket':'databricks-corp-training/iot_data/', 'mount_folder':'/mnt/datariders'}
# MAGIC ]
# MAGIC 
# MAGIC for mount_point in mounts_list:
# MAGIC   bucket = mount_point['bucket']
# MAGIC   mount_folder = mount_point['mount_folder']
# MAGIC   try:
# MAGIC     dbutils.fs.ls(mount_folder)
# MAGIC     dbutils.fs.unmount(mount_folder)
# MAGIC   except:
# MAGIC     pass
# MAGIC   finally: #If MOUNT_FOLDER does not exist
# MAGIC     dbutils.fs.mount("s3a://"+ ACCESSY_KEY_ID + ":" + SECERET_ACCESS_KEY + "@" + bucket,mount_folder)
