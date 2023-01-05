import streamlit as st
import pandas as pd
import requests
import glob
import os
import re
from newspaper import Article, fulltext, Config, ArticleException
from mechanicalsoup import StatefulBrowser
import newspaper
import tldextract
import random
import time
import numpy as np
import csv
import regex
import io
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.cloud import bigquery



'''
FUNCTIONS
'''

#Diffbot API
user =      'katia.bedolla@porternovelli.mx'
API_TOKEN = '7a7668b8111a6e4d5750c12a8c93b56d'

class DiffbotClient(object):

    base_url = 'http://api.diffbot.com/'

    def request(self, url, token, api, fields=None, version=3, **kwargs):
        """
        Returns a python object containing the requested resource from the diffbot api
        """
        params = {"url": url, "token": token}
        if fields:
            params['fields'] = fields
        params.update(kwargs)
        response = requests.get(self.compose_url(api, version), params=params)
        response.raise_for_status()
        return response.json()

    def compose_url(self, api, version_number):
        """
        Returns the uri for an endpoint as a string
        """
        version = self.format_version_string(version_number)
        return '{}{}/{}'.format(self.base_url, version, api)
    @staticmethod
    def format_version_string(version_number):
        """
        Returns a string representation of the API version
        """
        return 'v{}'.format(version_number)


def get_content_diffbot(url):
    diffbot = DiffbotClient()
    token = API_TOKEN
    api = "analyze"
    try:
        response = diffbot.request(url, token, api)
        if 'objects' in response:
            if len(response['objects'])>0:
                if 'text' in response['objects'][0]:
                    return response['objects'][0]['text']
                else:
                    return "No Content"
            else:
                return "Empty URL, Nothing found"
        else:
            return "Empty URL, Nothing found"
    except:
        return "Something went wrong with url"

user_agent_list = [
   #Chrome
     'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0',

    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

##### ----- Function: Define the function using newspaper3k ----- #####
def get_content_news(url): 
      user_agent = random.choice(user_agent_list)
      config = Config()
      config.browser_user_agent = user_agent
      time.sleep(.05)  
      a  =  Article(url, config=config)
      try:
           a.download()
           a.parse()
           paragraphs = a.text
           paragraphs = re.sub(r'<a href=.+?(?=)>|<br/>|\\xa0|\n|</a>|\xa0|<strong>|</strong>|<br/>•|<i(.*?)</i>|<img(.*?)>','',str(paragraphs))
           if type(paragraphs)==str and  len(paragraphs) >0:
                  print("URL Content from {} is correct".format(url))
                  return paragraphs
           elif paragraphs == '' or  type(paragraphs) == newspaper.article.ArticleException:
                 ext_diff= get_content_diffbot(url) 
                 print("URL Content from {} is correct from diffbot".format(url))
                 return ext_diff
      except Exception as exce:
             print("URL Content from {} is OtherError".format(url))
             try:
                 ext_diff= get_content_diffbot(url)
                 print("URL Content from {} is correct from diffbot".format(url))
                 return ext_diff
             except: 
                 print("URL Content from {} is OtherError".format(url))

'''
This function takes the Global News records and extracts the subdomain, domain, and suffix of the URL. 
This information is already contained in CIMS, 
so this function is used to retrieve it for use in the current context.
'''
def get_domain(url):
    try:
        ext = tldextract.extract(url)
        domain = ext.domain
        suffix = ext.suffix
        subdomain = ext.subdomain
        if subdomain == 'www':
            subdomain = ''
        else:
            subdomain = subdomain
        return "{}{}.{}".format(subdomain, domain, suffix)
    except Exception as e:
        return "error: {}".format(e)

"""
This function is used to extract the text content from a given URL using the StatefulBrowser module. 
It takes in a row of data from a pandas DataFrame containing the "Mention URL" column and returns the extracted text content. 
In case of any errors, it returns a string with the error message.
"""

"""
PARAMETERS:
row (dict): A dictionary containing the data for a single row in a dataframe. It should contain the key "Mention URL", 
which represents the URL of the webpage that needs to be scraped.
RETURNS:
str: A string representing the content of the webpage, with certain HTML tags removed. 
If there is an error while scraping the webpage, returns a string with the error message.
"""
def get_body(row):
    try:
        start = time.time()
        browser = StatefulBrowser(soup_config={"features": "lxml"})
        browser.open(row["Url"])
        content = browser.get_current_page().find_all("p")
        # Quitar etiquetas HTML no deseadas
        content = re.sub(r"<a(.*?)>|</a>|<br(.*?)>|<img(.*?)>|</p(.*?)>|<b(.*?)>|</b(.*?)>|<i(.*?)>|</i(.*?)>|<font(.*?)>|</font(.*?)>|<small(.*?)>|</small(.*?)>|<span(.*?)>|</span>", "", str(content))
        # Quitar atributos y otros caracteres no deseados
        content = (
            content.replace("=", "")
            .replace("[", "")
            .replace("]", "")
            .replace('"', "")
            .replace("\\xa0", " ")
            .replace("\\n", " ")
            .replace("\n", " ")
            .replace("\t", " ")
        )
        print(row["Url"])
        #print(content)
        end = time.time()
        print(f"Process completed. The execution time was: {end-start}")
        return content
    except Exception as e:
        print(e)
        return f"error {e}"


"""

This function receives an integer month as an input and returns a list with the name of the month, 
its corresponding quarter, half year and month number in a string format. If the input month is None, 
it returns a list of 'None' strings.

PARAMETERS:

month: integer (1-12) or None. The month number that is being queried.
RETURN:

list: a list of strings containing the month name, month number, quarter and half year.
"""
def find_quarters(month):
    try:
        if month =='1': 
            return ['January','07','Q3','H2']
        elif month =='2': 
            return ['February','08','Q3','H2']
        elif month =='3': 
            return ['March','09','Q3','H2']
        elif month =='4': 
            return ['April','10','Q4','H2']
        elif month =='5': 
            return ['May','11','Q4','H2']
        elif month =='6': 
            return ['June','12','Q4','H2']
        elif month =='7': 
            return ['July','01','Q1','H1']
        elif month =='8': 
            return ['August','02','Q1','H1']
        elif month =='9': 
            return ['September','03','Q1','H1']
        elif month =='10': 
            return ['October','04','Q2','H1']
        elif month =='11': 
            return ['November','05','Q2','H1']
        elif month =='12': 
            return ['December','06','Q2','H1']
        elif month == None:
            return ['None','None','None','None']
    except Exception as e:
        print(e)
        return f"error {e}"



"""
This function checks the delimiter used in each .csv file in the specified folder.

PARAMETERS:

folder: string with the path to the folder containing the .csv files
OUTPUT:

Prints a message indicating whether each .csv file is separated by commas or semicolons, 
or an error message if there was a problem reading the file.
"""
def check_csv_separator(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            try:
                with open(folder + "/" + filename) as csv_file:
                    dialect = csv.Sniffer().sniff(csv_file.read())
                    if dialect.delimiter == ",":
                        print(f"El archivo {filename} está separado por comas.")
                    elif dialect.delimiter == ";":
                        print(f"El archivo {filename} está separado por punto y coma.")
                    else:
                        print(f"El archivo {filename} tiene un separador desconocido.")
            except Exception as e:
                print(f"Error al leer el archivo {filename}: {e}")


"""
merge_csv_files(bucket_name: str, pattern: str) -> pd.DataFrame

This function merges multiple CSV files in a GCS bucket with the given pattern into a single dataframe.

PARAMETERS

bucket_name: str - name of the GCS bucket
pattern: str - pattern to use when selecting the files to merge
RETURNS

df_merged: pd.DataFrame - merged dataframe of all CSV files in the bucket
"""
def merge_csv_files(bucket_name, pattern):
    # Create GCS Client
    gcs_client = storage.Client()
    
    # Download GCS Files
    def download_file(bucket_name, file_name):
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.download_to_filename(file_name)
    
    # Download Bucket Files
    bucket = gcs_client.get_bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=pattern):
        download_file(bucket_name, blob.name)
    
    # Read Files y Concatenate All As a DataFrame
    all_files = glob.glob(os.path.join(path, pattern))
    df_from_each_file = (pd.read_csv(f, on_bad_lines= 'skip', sep= ',') for f in all_files)
    df_merged = pd.concat(df_from_each_file, ignore_index=True, sort=False)
    
    return df_merged


"""
Function to read and concatenate all .xlsx files in a GCS bucket.

Parameters:

storage_client: A client to connect to GCS.
BUCKET_NAME: Name of the GCS bucket to read from.
FOLDER_NAME: Name of the folder within the bucket to read from.
Returns:

df_eml: A pandas dataframe containing the data from all .xlsx files in the specified folder of the GCS bucket.
"""
creds = Credentials.from_authorized_user_info(info=None)
storage_client = storage.Client(credentials=creds)
BUCKET_NAME = 'dev-ms-rino678'
FOLDER_NAME = 'documentation'
bucket = storage_client.get_bucket(BUCKET_NAME)
filename_eml = list(bucket.list_blobs(prefix=FOLDER_NAME))
for name in filename_eml:
    print(name.name)
    print(name.size)

df_eml = pd.DataFrame()
for name in filename_eml:
    if name.name.endswith('.xlsx'):
        df_eml = pd.read_excel(io.BytesIO(bucket.blob(name.name).download_as_string()), index_col=None)
        print(df_eml.shape)
df_eml.head(3)


"""
This code is creating a new dataframe called "media_list_subset" by subsetting the columns
of another dataframe called "df_eml". It then prints the shape of the new dataframe
and displays the first few rows of it.
"""
media_list_subset = df_eml[['Outlet',
'Media Type',
'Language',
'Country',
'Impressions',
'Host',
'Subsidiary',
'Tier',
'CIMS',
'CIMS Version ML',
'Is Media List',
'FY23 Sub']]
print(media_list_subset.shape)
media_list_subset.head()


"""
Function to download and merge all csv files in a given folder in a Google Cloud Storage bucket.

Parameters:
bucket_name (str): The name of the bucket in GCS.
folder_name (str): The name of the folder in the bucket where the csv files are located.
"""

"""
Para obtener las credenciales de la cuenta de servicio necesitas seguir estos pasos:

Ve al panel de Google Cloud y selecciona la opción "Security".
Haz clic en el botón "Set up a new service account" para crear una nueva cuenta de servicio.
Asigna un nombre a la cuenta de servicio y selecciona "Project -> Editor" como el rol del usuario.
Haz clic en el botón "Create key" para generar una clave de la cuenta de servicio.
Selecciona "JSON" como el formato de la clave y haz clic en "Create".
Guarda el archivo de clave en un lugar seguro.
Asigna la ruta del archivo de clave a la variable 'info' en la función 'from_authorized_user_info' y usa esa variable para crear las credenciales.
Por ejemplo:

creds = Credentials.from_authorized_user_info(info={"client_id": "abcdefg.apps.googleusercontent.com", "client_secret": "hijklmnopqrstuvwxyz", "refresh_token": "1234567890"})
"""
#creds = Credentials.from_authorized_user_info(info=None)
#storage_client = storage.Client(credentials=creds)
storage_client = storage.Client.from_service_account_json(r'/home/mldevops2/streamlit_cims_app/storage_client.json')
BUCKET_NAME = 'streamlit_app_cims'
FOLDER_NAME = 'cims_all_company'
bucket = storage_client.get_bucket(BUCKET_NAME)
filename_cims = list(bucket.list_blobs(prefix=FOLDER_NAME))
for name in filename_cims:
    print(name.name)
    print(name.size)

blobs = []
df = pd.DataFrame()
for name in filename_cims:
    if name.name.endswith('.csv'):
        df = df.append(pd.read_csv(io.BytesIO(bucket.blob(name.name).download_as_string()), sep=',', encoding='utf-8', error_bad_lines=False))
        blobs.append(df)
        cims_raw_data = pd.concat(blobs)
        print(cims_raw_data.columns)
        cims_raw_data.drop_duplicates(subset='Url', keep='first', inplace=True)
        print(cims_raw_data.shape)
cims_raw_data.head()

cims_raw_data.rename(columns={'Outlet Name': 'Outlet'}, inplace=True)
cims_raw_data = pd.merge(cims_raw_data, media_list_subset, how='left', on='Outlet')
cims_raw_data.drop_duplicates(subset=['Url'], keep='first', inplace=True)
cims_raw_data['Source'] = 'CIMS'



"""
This code iterates through the rows of the cims_raw_data DataFrame and,
for rows with a Source value of "CIMS", it retrieves the content of the news article
at the URL specified in the Url column and stores it in the Content column for that row. 
It also prints the elapsed time, row index, and final shape of the DataFrame.
"""
start = time.time()
try:
    for index,row in cims_raw_data.iterrows():
        if (row['Source']=='CIMS'):
            cims_raw_data.at[index, 'Content'] =  get_content_news(row['Url'])
            print(index)
except Exception as e:
    print(e)
    pass
end = time.time()
print(end)

print(cims_raw_data.shape)
cims_raw_data.head(3)


for index, row in cims_raw_data.iterrows():
    if "Something went wrong with url" in str(row["Content"]):
        cims_raw_data.at[index, "Content"] = get_body(row)


cims_raw_data['Content_Length'] = cims_raw_data['Content'].str.len()
cims_raw_data.head(3)

cims_raw_data['Content_Length_Check'] = np.where(cims_raw_data['Content_Length'] < 400, 'CHECK', 'OK')
cims_raw_data.head(3)

# Retrieve the credentials for the service account
creds = Credentials.from_authorized_user_info(info=None)

# Define the schema for the table
table_schema = [
    {'name': 'columna1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'columna2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'columna3', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'columna4', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
]

# Generate the SQL code to create the table
cims_raw_data.to_gbq(destination_table='mi_proyecto.mi_dataset.mi_tabla',
          project_id='mi_proyecto',
          if_exists='replace',
          table_schema=table_schema,
          credentials=creds)



