#%%
print('Importando as bibliotecas...')
import msal
import requests
import json
import pandas as pd
from pandas import json_normalize
import os
from datetime import datetime
import time,os
import boto3

#%%
print('Criando as Variáveis de ambiente....')
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

try:
    print('Criando a client do Boto3...')
    AWS_REGION = "us-east-1"
    client = boto3.client("s3", 
                            region_name="us-east-1",
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    
    print('Client do Boto3 criado!')
except:
    raise("error")

#%%
print('Declarando as variáveis locais!')

current_datetime = datetime.now()
tenant = os.getenv('tenant_azure')
client_id = os.getenv('client_id_azure')
username = os.getenv('GED_USER')
password = os.getenv('GED_PASSWORD')
authority_url = 'sua authority url aqui'
scope = ["https://analysis.windows.net/powerbi/api/.default"]

#%%
print('Imprimindo cada uma das variáveis de ambiente declaradas', client_id, tenant)
print('Usando a biblioteca MSAL para pegar um Token')


print('Gerando Token...')
app = msal.PublicClientApplication(client_id, authority=authority_url)
result = app.acquire_token_by_username_password(username=username,password=password,scopes=scope)
print('Testando se o Token foi obtido com sucesso e inserindo-o na PowerBI REST API')

if 'access_token' in result:
    access_token = result['access_token']
    header = {'Content-Type':'application/json','Authorization': f'Bearer {access_token}'}
    print(header)
    print("Token Gerado com sucesso.")
else:
    print(f"Erro. Detalhes: {result.get('error_description')}")
    raise ValueError ("Erro ao obter o Token.")

#%%
print('Gerando um JSON com a relação de Grupos (Workspaces)')
url_groups = 'https://api.powerbi.com/v1.0/myorg/groups'
api_out = requests.get(url=url_groups, headers=header)
json_groups = api_out.json()
print(json_groups)
print('Json gerado.')
json_groups_string = json.dumps(json_groups)
print(json_groups_string)

# Gerando um DF para gerar os DF's dependentes:
df_groups = json.dumps(json_groups)
df_groups = pd.read_json(df_groups, lines = True, orient="columns")
df_groups = json_normalize(df_groups['value'])
stacked_df = df_groups.stack(dropna=False)
stacked_df = pd.Series.to_frame(stacked_df)
df_groups = pd.DataFrame()

for index, row in stacked_df.iterrows():
    df_groups_aux = json_normalize(row)
    df_groups = pd.concat([df_groups,df_groups_aux], ignore_index=True)

print('DF de Grupos criado.')

file_name = f"json_groups{current_datetime.strftime('%Y-%m-%d')}.json"
# %%
print('Gerando um JSON com a relação dos apps por workspaces')
print('Relação dos Apps por Workspaces.')

url_apps = 'https://api.powerbi.com/v1.0/myorg/apps'
api_out = requests.get(url=url_apps, headers=header)
json_apps = api_out.json()
print('Json gerado.')
json_apps_string = json.dumps(json_apps)
print(json_apps_string)

# Gerando um DF para gerar os DF's dependentes:
df_apps = json.dumps(json_apps)
df_apps = pd.read_json(df_apps, lines = True, orient="columns")
df_apps = json_normalize(df_apps['value'])
stacked_df = df_apps.stack(dropna=False)
stacked_df = pd.Series.to_frame(stacked_df)
df_apps = pd.DataFrame()

for index, row in stacked_df.iterrows():
    df_apps_aux = json_normalize(row)
    df_apps = pd.concat([df_apps,df_apps_aux], ignore_index=True)

print('DF de Apps criado.')

file_name = f"json_apps{current_datetime.strftime('%Y-%m-%d')}.json"

#%%
print('Gerando um JSON com a relação das atualizações de cada dataset.')
print('Relação dos Refreshables.')

print('Gerando JSON....')
url_df_refreshables = 'https://api.powerbi.com/v1.0/myorg/capacities/refreshables?$top=9999'
api_out = requests.get(url=url_df_refreshables, headers=header)
json_refreshables = api_out.json()
print('Json gerado.')
json_refreshables_string = json.dumps(json_refreshables)
print(json_refreshables_string)

file_name = f"json_refreshables{current_datetime.strftime('%Y-%m-%d')}.json"

# %%
print('Gerando um JSON com a relação dos apps/relatórios')
print('Usando todas as IDs de APP para usar o comando da API Reports.Read.All')

df_apps_reports_2 = pd.DataFrame()

for index, line in df_apps.iterrows():
    id_app = line['id']
    url_apps_reports = f"https://api.powerbi.com/v1.0/myorg/apps/{id_app}/reports"
    api_out = requests.get(url=url_apps_reports, headers=header)
    print(api_out.json())
    print('Json gerado.')
    df_apps_reports = json.dumps(api_out.json())
    df_apps_reports = pd.read_json(df_apps_reports, lines = True, orient="columns")
    df_apps_reports = json_normalize(df_apps_reports['value'])
    stacked_df = df_apps_reports.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    for index, row in stacked_df.iterrows():
        df_apps_reports_aux = json_normalize(row)
        df_apps_reports_aux['nome_app'] = line['name']
        df_apps_reports_aux['id_app'] = line['id']
        df_apps_reports_2 = pd.concat([df_apps_reports_2,df_apps_reports_aux], ignore_index=True)
          
df_apps_reports_2 = pd.DataFrame(df_apps_reports_2)

# Convertendo em uma String JSON
json_list_apps_reports_string = df_apps_reports_2.to_json(orient='index')
print('JSON de APPS/REPORTS criado.') 

file_name = f"json_apps_reports{current_datetime.strftime('%Y-%m-%d')}.json"

#%%
print('Gerando o JSON de Relação de Relatórios por GROUP')

df_groups_reports_2 = pd.DataFrame()

for index, line in df_groups.iterrows():
    groupId = line['id']
    url_df_groups_reports = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports"
    api_out = requests.get(url=url_df_groups_reports, headers=header)
    print(api_out.json())
    print('Json gerado.')
    json_groups_reports = api_out.json()
    df_groups_reports = json.dumps(api_out.json())
    df_groups_reports = pd.read_json(df_groups_reports, lines = True, orient="columns")
    df_groups_reports = json_normalize(df_groups_reports['value'])
    stacked_df = df_groups_reports.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    for index, row in stacked_df.iterrows():
        df_groups_reports_aux = json_normalize(row)
        df_groups_reports_aux['nome_group'] = line['name']
        df_groups_reports_aux['id_group'] = line['id']
        df_groups_reports_2 = pd.concat([df_groups_reports_2, df_groups_reports_aux], ignore_index=True)

df_groups_reports_2 = pd.DataFrame(df_groups_reports_2)

# Convertendo em uma String JSON
json_list_groups_reports_string = df_groups_reports_2.to_json(orient='index')
print('JSON de GROUPS/REPORTS criado.') 

file_name = f"json_groups_reports{current_datetime.strftime('%Y-%m-%d')}.json"

# %%
print('Gerando o JSON de Relação de Datasets por GROUP')

df_datasets_groups_2 = pd.DataFrame()
for index, line in df_groups.iterrows():
    groupId = line['id']
    url_df_datasets_groups = f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports"
    api_out = requests.get(url=url_df_datasets_groups, headers=header)
    print(api_out.json())
    print('Json gerado.')
    df_datasets_groups = json.dumps(api_out.json())
    df_datasets_groups = pd.read_json(df_datasets_groups, lines = True, orient="columns")
    df_datasets_groups = json_normalize(df_datasets_groups['value'])
    stacked_df = df_datasets_groups.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    for index, row in stacked_df.iterrows():
        df_datasets_groups_aux = json_normalize(row)
        df_datasets_groups_aux['nome_group'] = line['name']
        df_datasets_groups_aux['id_group'] = line['id']
        df_datasets_groups_2 = pd.concat([df_groups_reports_2, df_groups_reports_aux], ignore_index=True)
        
df_datasets_groups_2 = pd.DataFrame(df_datasets_groups_2)

# Convertendo em uma String JSON
json_list_datasets_groups_string = df_datasets_groups_2.to_json(orient='index')
print('JSON de DATASETS/GROUPS criado.') 

file_name = f"json_datasets_groups{current_datetime.strftime('%Y-%m-%d')}.json"

#%%
print("Fazendo o Upload dos JSONs gerados para a S3:")

dict_json_aws = {
    'json_groups_reports': [json_list_groups_reports_string, 'groups_reports'],
    'json_apps_reports': [json_list_apps_reports_string, 'apps_reports'],
    'json_apps': [json_apps_string, 'apps'],
    'json_groups': [json_groups_string, 'groups'],
    'json_datasets_groups': [json_list_datasets_groups_string, 'datasets_groups'],
    'json_refreshables': [json_refreshables_string, 'refreshables']
}

for i,j in dict_json_aws.items():
    file_name = f"{i}{current_datetime.strftime('%Y-%m-%d')}.json"

    print(f'Fazendo o Upload do JSON de {j[1]} para a S3:')
    try:
        bucket_name = 'seu bucket name'
        object_key = f'raw-zone/api/dit/powerbi/{j[1]}/{file_name}' # Caminho no S3

        client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body= j[0]
        )
    except:
       raise("error")

print('Script finalizado!!!')
#%%
