# Databricks notebook source
# MAGIC %md
# MAGIC # Regular Expressions in action

# COMMAND ----------

# MAGIC %md
# MAGIC Import the 're' library that helps in resolving regular expressions in Python

# COMMAND ----------

import re

# COMMAND ----------

# MAGIC %md
# MAGIC Regex have the capability to match simple expressions using ordinary characters as well as complex patterns using special characters.
# MAGIC 
# MAGIC - **Ordinary characters include simple alphabets, numbers and symbols.**
# MAGIC 
# MAGIC Ordinary characters are used to get exact matches e.g. if you wanted to wanted to find the occurences of the term 'Python' in some text then your regex would be -->'*Python*'.
# MAGIC 
# MAGIC - **Special characters allow you to create generic pattern in text that are more like a 'Closest match'.**
# MAGIC 
# MAGIC For example if you want to match an email address then you cannot specify an exact match since people have a different emails. However there is a pattern that you can use to your benefit. proper emails will always have an '**@**' symbol in the middle and end with '**.com**'. Let's see how we can find this pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC Generate some random text

# COMMAND ----------

#random text with emails
txt ='''We have contacted Mr. Jhon Doe and havbe confirmed that he will be joining us for the meeting this evening. If you would like to contact him yourself you can call him on +1-415-5552671 or email him at jhon.doe@outlook.com. We also have the contact details of his assistant, you can contact him in case Mr. Doe does not respond. The assistants email id is jack_smith12@outlook.com.'''

# COMMAND ----------

# MAGIC %md
# MAGIC To find the provided pattern we use the *re.search()* function.

# COMMAND ----------

plain_text = re.search('assistant', txt) #find the word assistant in the above text

# COMMAND ----------

# MAGIC %md
# MAGIC to find the word 'assistant' we simply used it as an expression.
# MAGIC 
# MAGIC the *re.seach()* function returns the first occurence of th eprovided expresssion as well as its indexes.

# COMMAND ----------

plain_text #print out the result

# COMMAND ----------

print(plain_text.group()) #print just the match

# COMMAND ----------

print(plain_text.span()) #print indexes where the match was found

# COMMAND ----------

# MAGIC %md
# MAGIC Now lets try a more complex pattern and find the email addresses in the text

# COMMAND ----------

email = re.search(r'\S+@{1}\S+(.com){1}',txt) #regex to find an email address

# COMMAND ----------

print(email) #found the first occuring email

# COMMAND ----------

# MAGIC %md
# MAGIC let's understand the regex a little bit.
# MAGIC 
# MAGIC - \S : Finds a non-whitespace character
# MAGIC - \+ : Specifies to find 1 or more non-whitespace occurences
# MAGIC - @ : Exact match, specifies to find a '@' symbol.
# MAGIC - {1} : specifies to find only 1 '@' symbol.
# MAGIC - \S : Again specifies to find non-whitespace characters.
# MAGIC - \+ : Find atleast 1 non-white space characters.
# MAGIC - [.com] : Find exact match for .com
# MAGIC - {1} : Find exactly one occurence for '.com'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find all occurences

# COMMAND ----------

# MAGIC %md
# MAGIC if we want to extract all occurences of the provided regex, then we use the *re.findall()* function

# COMMAND ----------

emails = re.findall(r'\S+@{1}\S+(?:\.com)', txt) #finding all emails

# COMMAND ----------

print(emails)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Substitue Expression

# COMMAND ----------

# MAGIC %md
# MAGIC We can substitue the given expression with a string of our own using the *re.sub()* function.

# COMMAND ----------

substituted_string = re.sub(r'\S+@{1}\S+(.com){1}', '', txt) #remove emails from the given text.

# COMMAND ----------

print(substituted_string)

# COMMAND ----------

# MAGIC %md
# MAGIC Job Done!

# COMMAND ----------

# MAGIC %md
# MAGIC This feature can be used to redact documents. 
# MAGIC 
# MAGIC Say we want to remove emails from a text so that no confidentional contact information is exposed.
# MAGIC 
# MAGIC We can simple substitue it with an \<email\> tag

# COMMAND ----------

redacted = re.sub('\S+@{1}\S+(.com){1}', '<email>', txt)

# COMMAND ----------

print(redacted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple expressions in a single line.

# COMMAND ----------

# MAGIC %md
# MAGIC Say we want to find email as well as the time mentioned in the meeting. 
# MAGIC 
# MAGIC We can specify an OR expression to tell python to match either expression1 or expression2 or both.

# COMMAND ----------

re.findall('\S+@{1}\S+[.com]{1}|\+[0-9]{1}-[0-9]{3}-[0-9]{7}', txt)

# COMMAND ----------

redacted = re.sub('\S+@{1}\S+[.com]{1}|\+[0-9]{1}-[0-9]{3}-[0-9]{7}', '<confidential>', txt)

# COMMAND ----------

print(redacted)

# COMMAND ----------


