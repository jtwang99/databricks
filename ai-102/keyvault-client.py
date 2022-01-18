# Databricks notebook source
dbutils.widgets.text(name = "pythonTextWidget", defaultValue = "this is a test")
userText = dbutils.widgets.get("pythonTextWidget")

# COMMAND ----------

#from dotenv import load_dotenv
import os
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential


def main():
    global cog_endpoint
    global cog_key

    try:
        # Get Configuration Settings
        #load_dotenv()
        #cog_endpoint = os.getenv('COG_SERVICE_ENDPOINT')
        cog_endpoint = 'https://jtwang.cognitiveservices.azure.com/'
        key_vault_name = 'jtwang'
        #key_vault_name = os.getenv('KEY_VAULT')
        app_tenant = '9756f1f1-a54e-47f6-8a06-b29f1999b1d9'
        #app_tenant = os.getenv('TENANT_ID')
        app_id = '7d442551-b311-425e-909d-b030b2641206'
        app_password = 'i7fYkG29C4FXLWHAp69z-t0gRdRYAgMHd~'
        #app_id = os.getenv('APP_ID')
        #app_password = os.getenv('APP_PASSWORD')

        # Get cognitive services key from keyvault using the service principal credentials
        key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
        credential = ClientSecretCredential(app_tenant, app_id, app_password)
        keyvault_client = SecretClient(key_vault_uri, credential)
        secret_key = keyvault_client.get_secret("Cognitive-Services-Key")
        cog_key = secret_key.value

        # Get user input (until they enter "quit")
        language = GetLanguage(userText)
        print('Language:', language)
#         while userText.lower() != 'quit':
#             userText = input('\nEnter some text ("quit" to stop)\n')
#             if userText.lower() != 'quit':
#                 language = GetLanguage(userText)
#                 print('Language:', language)

    except Exception as ex:
        print(ex)

def GetLanguage(text):

    # Create client using endpoint and key
    credential = AzureKeyCredential(cog_key)
    client = TextAnalyticsClient(endpoint=cog_endpoint, credential=credential)

    # Call the service to get the detected language
    detectedLanguage = client.detect_language(documents = [text])[0]
    return detectedLanguage.primary_language.name


if __name__ == "__main__":
    main()

# COMMAND ----------


