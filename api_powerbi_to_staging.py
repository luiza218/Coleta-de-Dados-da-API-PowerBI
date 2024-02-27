#%%
print('Importando as bibliotecas...')
from datetime import date, datetime
from time import time
import re, os, io
import pandas as pd
import json
from pandas import json_normalize
import pyarrow as pa
import pyarrow.parquet as pq
# AWS
import boto3

#%%
print('Criando as Variáveis de ambiente....')
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

try:
    print('Criando a client do Boto3...')
    s3_client = boto3.client(
        's3', 
        region_name='us-east-1',
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key 
    )
    print('Client do Boto3 criado!')
except Exception as e:
    print(e)

#%%
date_str = datetime.today().strftime('%Y-%m-%d')
bucket_name = 'seu bucket name aqui'

#%%
print('Criando os arquivos locais:')
try:
    ARQUIVO_LOCAL_APPS_REPORTS = f'json_apps_reports{date_str}.json'
    ARQUIVO_LOCAL_APPS = f'json_apps{date_str}.json'
    ARQUIVO_LOCAL_DATASETS_GROUPS = f'json_datasets_groups{date_str}.json'
    ARQUIVO_LOCAL_GROUPS_REPORTS = f'json_groups_reports{date_str}.json'
    ARQUIVO_LOCAL_GROUPS = f'json_groups{date_str}.json'
    ARQUIVO_LOCAL_REFRESHABLES = f'json_refreshables{date_str}.json'

    print('Arquivos Locais Criados.')
except Exception as e:
    print(e)
    
#%%
print('Declarando o caminho dos JSONs na Raw-Zone para realizar o download...')
try:
    object_key_apps_reports = f'raw-zone/api/dit/powerbi/apps_reports/json_apps_reports{date_str}.json'
    object_key_groups = f'raw-zone/api/dit/powerbi/groups/json_groups{date_str}.json'
    object_key_apps = f'raw-zone/api/dit/powerbi/apps/json_apps{date_str}.json'
    object_key_refreshables = f'raw-zone/api/dit/powerbi/refreshables/json_refreshables{date_str}.json'
    object_key_groups_reports = f'raw-zone/api/dit/powerbi/groups_reports/json_groups_reports{date_str}.json'
    object_key_datasets_groups = f'raw-zone/api/dit/powerbi/datasets_groups/json_datasets_groups{date_str}.json'
    
    print('Caminho dos JSONs na Raw-Zone criados.')
except Exception as e:
    print(e)

#%%
try:
    print('\nFazendo download do JSON de APPS/REPORTS...')
    s3_client.download_file(bucket_name, object_key_apps_reports, ARQUIVO_LOCAL_APPS_REPORTS)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_APPS_REPORTS}")

    print('\nFazendo download do JSON de APPS...')
    s3_client.download_file(bucket_name, object_key_apps, ARQUIVO_LOCAL_APPS)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_APPS}")

    print('\nFazendo download do JSON de GRUPOS...')
    s3_client.download_file(bucket_name, object_key_groups, ARQUIVO_LOCAL_GROUPS)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_GROUPS}")

    print('\nFazendo download do JSON de GROUPS/REPORTS...')
    s3_client.download_file(bucket_name, object_key_groups_reports, ARQUIVO_LOCAL_GROUPS_REPORTS)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_GROUPS_REPORTS}")

    print('\nFazendo download do JSON de REFRESHABLES...')
    s3_client.download_file(bucket_name, object_key_refreshables, ARQUIVO_LOCAL_REFRESHABLES)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_REFRESHABLES}")

    print('\nFazendo download do JSON de DATASETS/GROUPS...')
    s3_client.download_file(bucket_name, object_key_datasets_groups, ARQUIVO_LOCAL_DATASETS_GROUPS)
    print(f"Arquivo baixado para: {ARQUIVO_LOCAL_DATASETS_GROUPS}")

    print('Arquivos foram baixados localmente com sucesso.')
except Exception as e:
    print(e)
#%%
print('Abrindo os arquivos para leitura....')

try:
    with open(ARQUIVO_LOCAL_APPS_REPORTS, 'r') as arquivo_apps_reports:
        apps_reports_json = json.load(arquivo_apps_reports)
    print('Arquivo de APPS/REPORTS aberto com sucesso.')

    with open(ARQUIVO_LOCAL_DATASETS_GROUPS, 'r') as arquivo_datasets_groups:
        datasets_groups_json = json.load(arquivo_datasets_groups)
    print('Arquivo de DATASETS/GROUPS aberto com sucesso.')

    with open(ARQUIVO_LOCAL_APPS, 'r') as arquivo_apps:
        apps_json = json.load(arquivo_apps)
    print('Arquivo de APPS aberto com sucesso.')

    with open(ARQUIVO_LOCAL_GROUPS, 'r') as arquivo_groups:
        groups_json = json.load(arquivo_groups)
    print('Arquivo de GROUPS aberto com sucesso.')

    with open(ARQUIVO_LOCAL_GROUPS_REPORTS, 'r') as arquivo_groups_reports:
        groups_reports_json = json.load(arquivo_groups_reports)
    print('Arquivo de GROUPS/REPORTS aberto com sucesso.')

    with open(ARQUIVO_LOCAL_REFRESHABLES, 'r') as arquivo_refreshables:
        refreshables_json = json.load(arquivo_refreshables)
    print('Arquivo de REFRESHABLES aberto com sucesso.')

    print('Todos os arquivos foram abertos para leitura com Sucesso!')
except Exception as e:
    print(e)

#%%
print('Criando o DF de Groups....')
try:
    groups_json = json.dumps(groups_json)
    df_groups = pd.read_json(groups_json, lines = True, orient="columns")
    df_groups = json_normalize(df_groups['value'])
    stacked_df = df_groups.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_groups = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_groups_aux = json_normalize(row)
        df_groups = pd.concat([df_groups,df_groups_aux], ignore_index=True)

    # Modificações
    df_groups = df_groups.drop(['isReadOnly', 'isOnDedicatedCapacity', 'capacityId', 'defaultDatasetStorageFormat'], axis=1)
except Exception as e:
    print(e)

df_groups.columns = ['id_workspace', 'tipo', 'nome_workspace']
print('DF de Groups criado!!!')

#%%
print('Criando o DF de Apps....')
try:
    apps_json = json.dumps(apps_json)
    df_apps = pd.read_json(apps_json, lines = True, orient="columns")
    df_apps = json_normalize(df_apps['value'])
    stacked_df = df_apps.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_apps = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_apps_aux = json_normalize(row)
        df_apps = pd.concat([df_apps,df_apps_aux], ignore_index=True)

    # Modificações
    df_apps = df_apps.drop(['publishedBy', 'users'], axis=1)
except Exception as e:
    print(e)

df_apps.columns = ['id', 'nome', 'lastupdate', 'descricao', 'id_workspace']

print('DF de Apps criado!!!')
#%%
print('Criando o DF de Refreshables....')
try:
    refreshables_json = json.dumps(refreshables_json)
    df_refreshables = pd.read_json(refreshables_json, lines = True, orient="columns")
    df_refreshables = json_normalize(df_refreshables['value'])
    stacked_df = df_refreshables.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_refreshables = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_refreshables_aux = json_normalize(row)
        df_refreshables = pd.concat([df_refreshables,df_refreshables_aux], ignore_index=True)

    # Modificações
    df_refreshables = df_refreshables.drop(['lastRefresh.refreshAttempts', 'lastRefresh.id', 'startTime', 'endTime', 'lastRefresh.serviceExceptionJson', 'lastRefresh.requestId', 'lastRefresh.extendedStatus', 'refreshSchedule.localTimeZoneId', 'medianDuration','refreshSchedule.notifyOption', 'configuredBy'], axis=1)
except Exception as e:
    print(e)

df_refreshables.columns = ['id_dataset', 'nome_dataset', 'tipo', 'refresh_contagem', 'refresh_falhas', 'media_duracao_refresh','refreshes_por_dia', 
                           'tipo_ultimo_refresh', 'hora_inicio_ultimo_refresh', 'hora_fim_ultimo_refresh', 'status_ultimo_refresh',
                           'dias_atribuidos_refresh', 'horarios_atribuidos_refresh', 'atualizacao_automatica_ativada']

# Convertendo os arrays das células em strings, concatenando
for index, line in df_refreshables.iterrows():
    df_refreshables.at[index, 'horarios_atribuidos_refresh'] = ','.join(line['horarios_atribuidos_refresh'])

for index, line in df_refreshables.iterrows():
    df_refreshables.at[index, 'dias_atribuidos_refresh'] = ','.join(line['dias_atribuidos_refresh'])

print('DF de Refreshables criado!!!')

#%%
print('Criando o DF de Relatórios/Groups....')
try:
    groups_reports_json = json.dumps(groups_reports_json)
    df_groups_reports = pd.read_json(groups_reports_json, lines = True, orient="columns")
    stacked_df = df_groups_reports.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_groups_reports = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_groups_reports_aux = json_normalize(row)
        df_groups_reports = pd.concat([df_groups_reports,df_groups_reports_aux], ignore_index=True)

    # Modificações
    df_groups_reports = df_groups_reports.drop(['datasetWorkspaceId','webUrl','reportType', 'embedUrl', 'isFromPbix', 'isOwnedByMe', 'users', 'subscriptions'], axis=1)
except Exception as e:
    print(e)

df_groups_reports.columns = ['id_report', 'nome_report', 'id_dataset', 'nome_workspace', 'id_workspace']

print('DF de Relatórios/Groups criado!!!')
#%%
print('Criando o DF de Relatórios/Apps....')
try:
    apps_reports_json = json.dumps(apps_reports_json)
    df_apps_reports = pd.read_json(apps_reports_json, lines = True, orient="columns")
    stacked_df = df_apps_reports.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_apps_reports = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_apps_reports_aux = json_normalize(row)
        df_apps_reports = pd.concat([df_apps_reports,df_apps_reports_aux], ignore_index=True)
    
    # Modificações
    df_apps_reports = df_apps_reports.drop(['webUrl','reportType', 'embedUrl', 'isOwnedByMe', 'originalReportObjectId', 'users', 'subscriptions', 'id_app', 'datasetId'], axis=1)
except Exception as e:
    print(e)

df_apps_reports.columns = ['id_report', 'nome_report', 'id_app', 'nome_app']

print('DF de Relatórios/Apps criado!!!')
#%%
print('Criando o DF de Datasets/Groups....')
try:
    datasets_groups_json = json.dumps(datasets_groups_json)
    df_datasets_groups = pd.read_json(datasets_groups_json, lines = True, orient="columns")
    stacked_df = df_datasets_groups.stack(dropna=False)
    stacked_df = pd.Series.to_frame(stacked_df)

    df_datasets_groups = pd.DataFrame()

    for index, row in stacked_df.iterrows():
        df_datasets_groups_aux = json_normalize(row)
        df_datasets_groups = pd.concat([df_datasets_groups,df_datasets_groups_aux], ignore_index=True)
    
    # Modificações
    df_datasets_groups = df_datasets_groups.drop(['datasetWorkspaceId','reportType', 'embedUrl', 'isFromPbix', 'isOwnedByMe', 'users', 'subscriptions', 'webUrl', 'users', 'subscriptions'], axis=1)
except Exception as e:
    print(e)

df_datasets_groups.columns = ['id_report', 'nome_report', 'id_dataset', 'nome_workspace', 'id_workspace']

print('DF de Datasets/Groups criado!!!')

# %%
print('Transformando os DFs em Parquets....')
try:
    # Groups
    tabela_groups = pa.Table.from_pandas(df_groups)
    groups_path = f'parquet_groups{date_str}.parquet'
    pq.write_table(tabela_groups, groups_path)

    # Apps
    tabela_apps = pa.Table.from_pandas(df_apps)
    apps_path = f'parquet_apps{date_str}.parquet'
    pq.write_table(tabela_apps, apps_path)

    # Refreshables
    tabela_refreshables = pa.Table.from_pandas(df_refreshables)
    refreshables_path = f'parquet_refreshables{date_str}.parquet'
    pq.write_table(tabela_refreshables, refreshables_path)

    # Groups/Reports
    tabela_groups_reports = pa.Table.from_pandas(df_groups_reports)
    groups_reports_path = f'parquet_groups_reports{date_str}.parquet'
    pq.write_table(tabela_groups_reports, groups_reports_path)

    # Apps/Reports
    tabela_apps_reports = pa.Table.from_pandas(df_apps_reports)
    apps_reports_path = f'parquet_apps_reports{date_str}.parquet'
    pq.write_table(tabela_apps_reports, apps_reports_path)

    # Datasets/Groups
    tabela_datasets_grupos = pa.Table.from_pandas(df_datasets_groups)
    datasets_groups_path = f'parquet_datasets_grupos{date_str}.parquet'
    pq.write_table(tabela_datasets_grupos, datasets_groups_path)
except Exception as e:
    print(e)

print('Parquets criados com sucesso!!!')
# %%
print('Subindo os parquets na staging zone da AWS....')

try:
    # Groups
    current_datetime = datetime.now()
    file_name_groups = f"parquet_groups{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_groups_staging = f'staging-zone/api/dit/powerbi/groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_groups}'

    with open(groups_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=object_key_groups_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)

print('Upload do arquivo PARQUET de GROUPS gravado na staging-zone com sucesso!!!') 

#%%
try:
    # Apps
    current_datetime = datetime.now()
    file_name_apps = f"parquet_apps{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_apps_staging = f'staging-zone/api/dit/powerbi/apps/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_apps}'

    with open(apps_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key_apps_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)

print('Upload do arquivo PARQUET de APPS gravado na staging-zone com sucesso!!!') 

#%%
try:
    # Refreshables
    current_datetime = datetime.now()
    file_name_refreshables = f"parquet_refreshables{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_refreshables_staging = f'staging-zone/api/dit/powerbi/refreshables/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_refreshables}'

    with open(refreshables_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key_refreshables_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)


print('Upload do arquivo PARQUET de REFRESHABLES gravado na staging-zone com sucesso!!!') 

#%%
try:
    # Groups/Reports
    current_datetime = datetime.now()
    file_name_groups_reports = f"parquet_groups_reports{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_groups_reports_staging = f'staging-zone/api/dit/powerbi/groups_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_groups_reports}'

    with open(groups_reports_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key_groups_reports_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)

print('Upload do arquivo PARQUET de GROUP/REPORTS gravado na staging-zone com sucesso!!!') 

#%%
try:
    # Apps/Reports
    current_datetime = datetime.now()
    file_name_apps_reports = f"parquet_apps_reports{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_apps_reports_staging = f'staging-zone/api/dit/powerbi/apps_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_apps_reports}'

    with open(apps_reports_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key_apps_reports_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)

print('Upload do arquivo PARQUET de APPS/REPORTS gravado na staging-zone com sucesso!!!')

#%%
try:
    # Datasets/Groups
    current_datetime = datetime.now()
    file_name_datasets_groups = f"parquet_datasets_groups{current_datetime.strftime('%Y-%m-%d')}.parquet"
    object_key_datasets_groups_staging = f'staging-zone/api/dit/powerbi/datasets_groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_datasets_groups}'

    with open(datasets_groups_path, 'rb') as parquet_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key_datasets_groups_staging,
            Body=parquet_file
        )
except Exception as e:
    print(e)

print('Upload do arquivo PARQUET de DATASETS/GROUPS gravado na staging-zone com sucesso!!!') 

print('Script finalizado!!!')
#%%

