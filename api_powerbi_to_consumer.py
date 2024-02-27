#%%
from datetime import datetime
import os
import boto3

#%%
bucket_name = 'seu bucket name'

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
except Exception as e:
    print(e)

print('Client do Boto3 criado!')

#%%
date_str = datetime.today().strftime('%Y-%m-%d')
current_datetime = datetime.now()

def hora_atual() -> str:
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')

#%%
file_name_groups = f"parquet_groups{current_datetime.strftime('%Y-%m-%d')}.parquet"
file_name_apps = f"parquet_apps{current_datetime.strftime('%Y-%m-%d')}.parquet"
file_name_refreshables = f"parquet_refreshables{current_datetime.strftime('%Y-%m-%d')}.parquet"
file_name_groups_reports = f"parquet_groups_reports{current_datetime.strftime('%Y-%m-%d')}.parquet"
file_name_apps_reports = f"parquet_apps_reports{current_datetime.strftime('%Y-%m-%d')}.parquet"
file_name_datasets_groups = f"parquet_datasets_groups{current_datetime.strftime('%Y-%m-%d')}.parquet"

#%%
key_staging_apps = f'staging-zone/api/dit/powerbi/apps/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_apps}'
key_staging_apps_reports = f'staging-zone/api/dit/powerbi/apps_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_apps_reports}'
key_staging_groups = f'staging-zone/api/dit/powerbi/groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_groups}'
key_staging_groups_reports = f'staging-zone/api/dit/powerbi/groups_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_groups_reports}'
key_staging_refreshables = f'staging-zone/api/dit/powerbi/refreshables/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_refreshables}'
key_staging_datasets_groups = f'staging-zone/api/dit/powerbi/datasets_groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}/{file_name_datasets_groups}'

#%%
consumer_uri_apps = f'consumer-zone/api/dit/powerbi/apps/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_apps = f"{consumer_uri_apps}/{file_name_apps}"

consumer_uri_apps_reports = f'consumer-zone/api/dit/powerbi/apps_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_apps_reports = f"{consumer_uri_apps_reports}/{file_name_apps_reports}"

consumer_uri_groups = f'consumer-zone/api/dit/powerbi/groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_groups = f"{consumer_uri_groups}/{file_name_groups}"

consumer_uri_groups_reports = f'consumer-zone/api/dit/powerbi/groups_reports/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_groups_reports = f"{consumer_uri_groups_reports}/{file_name_groups_reports}"

consumer_uri_datasets_groups = f'consumer-zone/api/dit/powerbi/datasets_groups/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_datasets_groups = f"{consumer_uri_datasets_groups}/{file_name_datasets_groups}"

consumer_uri_refreshables = f'consumer-zone/api/dit/powerbi/refreshables/fat/year={current_datetime:%Y}/month={current_datetime:%m}/day={current_datetime:%d}'
consumer_parquet_refreshables = f"{consumer_uri_refreshables}/{file_name_refreshables}"

#%%
#Copia o parquet de APPS da staging para a consumer
try:
    print('Copiando parquet de APPS da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_apps,
        Key = consumer_parquet_apps
    )
    print('Upload do parquet de APPS finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_apps = f'consumer-zone/api/dit/powerbi/apps/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de APPS...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_apps
        )
        print('Decodificando manifest de APPS...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de APPS não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de APPS...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_apps}\n'  
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_apps}\n'  
    print('Sobreescrevendo manifest de APPS na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_apps,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')

# %%
#Copia o parquet de APPS REPORTS da staging para a consumer
try:
    print('Copiando parquet de APPS REPORTS da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_apps_reports,
        Key = consumer_parquet_apps_reports
    )
    print('Upload do parquet de APPS REPORTS finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_apps_reports = f'consumer-zone/api/dit/powerbi/apps_reports/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de APPS REPORTS...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_apps_reports
        )
        print('Decodificando manifest de APPS REPORTS...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de APPS REPORTS não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de APPS REPORTS...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_apps}\n'        
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_apps_reports}\n'  
    print('Sobreescrevendo manifest de APPS REPORTS na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_apps_reports,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')

#%%
#Copia o parquet de GROUPS da staging para a consumer
try:
    print('Copiando parquet de GROUPS da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_groups,
        Key = consumer_parquet_groups
    )
    print('Upload do parquet de GROUPS finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_groups = f'consumer-zone/api/dit/powerbi/groups/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de GROUPS...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_groups
        )
        print('Decodificando manifest de GROUPS...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de GROUPS não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de GROUPS...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_groups}\n'        
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_groups}\n'  
    print('Sobreescrevendo manifest de GROUPS na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_groups,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')
# %%
#Copia o parquet de GROUPS REPORTS da staging para a consumer
try:
    print('Copiando parquet de GROUPS REPORTS da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_groups_reports,
        Key = consumer_parquet_groups_reports
    )
    print('Upload do parquet de GROUPS REPORTS finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_groups_reports = f'consumer-zone/api/dit/powerbi/groups_reports/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de GROUPS REPORTS...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_groups_reports
        )
        print('Decodificando manifest de GROUPS REPORTS...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de GROUPS REPORTS não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de GROUPS REPORTS...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_groups_reports}\n'        
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_groups_reports}\n'  
    print('Sobreescrevendo manifest de GROUPS REPORTS na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_groups_reports,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')
# %%
#Copia o parquet de DATASETS GROUPS da staging para a consumer
try:
    print('Copiando parquet de DATASETS GROUPS da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_datasets_groups,
        Key = consumer_parquet_datasets_groups
    )
    print('Upload do parquet de DATASETS GROUPS finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_datasets_groups = f'consumer-zone/api/dit/powerbi/datasets_groups/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de DATASETS GROUPS...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_datasets_groups
        )
        print('Decodificando manifest de DATASETS GROUPS...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de DATASETS GROUPS não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de DATASETS GROUPS...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_datasets_groups}\n'        
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_datasets_groups}\n'  
    print('Sobreescrevendo manifest de DATASETS GROUPS na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_datasets_groups,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')
# %%
#Copia o parquet de REFRESHABLES da staging para a consumer
try:
    print('Copiando parquet de REFRESHABLES da staging para consumer...', hora_atual(), '\n')
    response = s3_client.copy_object(
        Bucket = bucket_name,
        CopySource = bucket_name+'/'+key_staging_refreshables,
        Key = consumer_parquet_refreshables
    )
    print('Upload do parquet de REFRESHABLES finalizado com sucesso!!!  ', hora_atual(), '\n')
    #Registra no manifest link 
    try:
        key_manifest_refreshables = f'consumer-zone/api/dit/powerbi/refreshables/fat/_symlink_format_manifest/manifest'
        print('Baixando manifest link de REFRESHABLES...', hora_atual())
        manifest_obj = s3_client.get_object(
            Bucket=bucket_name,
            Key=key_manifest_refreshables
        )
        print('Decodificando manifest de REFRESHABLES...', hora_atual())
        manifest_contents = manifest_obj['Body'].read( ).decode('utf-8')
    except:
        print('Arquivo manifest de REFRESHABLES não foi encontrado. Criando novo arquivo manisfest...', hora_atual())
        manifest_contents = ''

    if manifest_contents == '':
        print('Adicionando nova linha no manifest de REFRESHABLES...', hora_atual())
        manifest_contents_new = f's3://{bucket_name}/{consumer_parquet_refreshables}\n'        
    else:
        manifest_contents_new = f'{manifest_contents}s3://{bucket_name}/{consumer_parquet_refreshables}\n'  
    print('Sobreescrevendo manifest de REFRESHABLES na consumer...', hora_atual())
    s3_client.put_object(
        Body=manifest_contents_new,
        Bucket=bucket_name,
        Key=key_manifest_refreshables,
    )
    print('Finalizado com sucesso!!!', hora_atual())
except:
    print('Dados obtidos não atendem ao conteudo necessario, favor verificar')

print('Script finalizado.')
#%%
