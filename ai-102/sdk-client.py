# Databricks notebook source
# MAGIC %pip install  azure.ai.textanalytics

# COMMAND ----------

# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install azure.core

# COMMAND ----------

#from dotenv import load_dotenv
import os
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

def main():
    global cog_endpoint
    global cog_key

    try:
        # Get Configuration Settings
        #load_dotenv()
        cog_endpoint = 'https://jtwang.cognitiveservices.azure.com/'
        cog_key = '83f7db3dd0fd4b0a88c1ab7172c7a3e6'
        # Get user input (until they enter "quit")
        userText ='這是測試'
        language = GetLanguage(userText)
        print('Language:', language)
        '''
        while userText.lower() != 'quit':
            userText = input('\nEnter some text ("quit" to stop)\n')
            if userText.lower() != 'quit':
                language = GetLanguage(userText)
                print('Language:', language)
'''
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


