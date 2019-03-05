import os
import boto3
import pandas as pd
import re
from io import StringIO


######################################
# Acces Fonctions
######################################
def find_csv_file_in_folder(folder_name, pattern=r"(.*\.(csv|CSV))"):
    trouve = False
    for file in os.listdir(folder_name):
        m = re.match(pattern, file)
        if m is not None:
            filenameCSV = m.groups()[0]
            return filenameCSV
            trouve = True
    if not(trouve):
        print("Aucun fichier csv trouve!")
        return None


def read_credentials_from_csv(
        filename='Utils/template_init_env.py',
        credential_type='Azure blob infos',
        verbose = False):
    try:
        with open(filename) as f:
            data = f.read()
        ls = [l for l in data.split('##') if credential_type in l]
        for command in ls[0].split('\n')[1:4]:
            if verbose:
                print(command)
            exec(command)
        print('Credentials read')
    except Exception as e:
        print(e)


######################################
# Functions for interactions with S3 (python)
######################################
def print_all_file_bucket(bucket_name="prd-datafactory-datali"):
    bucket = boto3.resource('s3').Bucket(bucket_name)
    for object in bucket.objects.all():
        print(object.key)


def get_bucket_content(bucket_name="prd-datafactory-datali"):
    bucket = boto3.resource('s3').Bucket(bucket_name)
    file_list = []
    for object in bucket.objects.all():
        file_list.append(object.key)
    return file_list


def S3_file_to_dataframe(
        filename,
        bucketname="prd-datafactory-datali",
        sep=',',
        header=0,
        encoding='utf-8',
        error_bad_lines=True,
        usecols=None,
        low_memory=False,
        engine='python',
        names=None):
    filepath = "s3://" + bucketname + "/" + filename

    if filename.split('.')[-1] in ['xls', 'xlsx']:
        return pd.read_excel(filepath, names=names)

    elif filename.split('.')[-1] in ['csv', 'tsv']:
        return pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            error_bad_lines=error_bad_lines,
            usecols=usecols,
            encoding=encoding,
            low_memory=low_memory,
            names=names)
    else:
        print(
            "Not able to import %s from %s (wrong file format)",
            filename,
            bucketname)
        return


def dataframe_to_S3(
        df,
        client_name,
        filename_AWS_S3,
        bucketname="prd-datafactory-datali"):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource = boto3.resource(client_name)
        body = csv_buffer.getvalue()
        s3_resource.Object(bucketname, filename_AWS_S3).put(Body=body)
        print('Dataframe sent to S3')
    except Exception as e:
        print("File transfer to S3 failed because of: %s" % e)


######################################
# Functions for interactions with S3 (spark)
######################################
def csv_read_pyspark(
        sqlContex,
        filename,
        bucketname="prd-datafactory-datali",
        delimit=',',
        isSchema=True,
        encoding="utf-8"):
    dataframe = (
        sqlContex
        .read
        .format("csv")
        .option("header", "true")
        .option("encoding", encoding)
        .option("inferSchema", isSchema)
        .option("nullValue", "None")
        .option("delimiter", delimit)
        .load("s3a://" + bucketname + "/" + filename)
    )
    return dataframe


def dataframeSpark_to_s3(
        df,
        delimit,
        ACCESS_KEY,
        SECRET_KEY,
        bucketname,
        filename_AWS):
    s3_key = bucketname + "/" + filename_AWS
    s3_path = "s3a://" + ACCESS_KEY + ":" + SECRET_KEY + "@" + s3_key

    (
        df.repartition(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", True)
        .option("nullValue", "None")
        .option("delimiter", delimit)
        .save(s3_path)
    )


######################################
# Push file to Blob Azure
######################################
def file_to_AzureBlob(
        filename,
        filename_blob,
        CONTAINER_NAME,
        ACCOUNT_NAME,
        ACCOUNT_KEY):
    cmd = 'az storage blob upload --file %s --name %s' + \
        ' --container-name %s --account-name %s --account-key %s' % \
        (
            filename,
            filename_blob,
            CONTAINER_NAME,
            ACCOUNT_NAME,
            ACCOUNT_KEY
        )
    try:
        os.system(cmd)
        print('File sent to Azure')
    except Exception as e:
        print(str(e))