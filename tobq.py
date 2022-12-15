from google.cloud import bigquery
import google.auth as gg
import pandas as pd
import os
import re
import unidecode

def generate_schema(schema_values):
    schema=[]
    modificacion=[]
    j=0
    l=0
    for i in (schema_values):
        if i is not None:
            campo=(i.replace(" ", ""))
            campo = (campo.replace(" ", ""))
            campo = (campo.replace("_", ""))
            campo = (campo.replace("\"", ""))
            campo = (campo.replace("/", ""))
            campo = (campo.replace("-", ""))
            campo = (campo.replace("(", ""))
            campo = (campo.replace(")", ""))
            campo = (campo.replace(":", ""))
            campo = (campo.replace(".", ""))
            campo = unidecode.unidecode(campo)
            campo = re.sub('[^a-zA-Z0-9 \n\.]', '', campo)
            campo = unidecode.unidecode(campo)
            if i=='':
                campo="vacio"+str(j)
                j=j+1
            for m in schema:
                if campo.lower()==str(m.name).lower():
                    campo=campo+str(l)
                    l=l+1

            modificacion.append({i: campo})
            schema.append(bigquery.SchemaField(campo, "STRING"))
    return schema,modificacion
def removeVacio(df):
    j=0
    df.pop("")
    return df

def grabar(dataset,tabla,file,columns):
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/xchavezx/PycharmProjects/migration/credentials/ultimo.json"
    if 'xls' in file:
        if len(columns) != 0:
            dfToBq = pd.read_excel(file, dtype=str,usecols=columns)
        else:
            dfToBq = pd.read_excel(file, dtype=str)
    if 'csv' in file:
        dfToBq=pd.read_csv(file,delimiter=';')

    schemaBq,columnas=generate_schema(dfToBq.columns)
    for i in columnas:
        dfToBq.rename(columns=i, inplace=True)
    credentials, project = gg.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    job_config = bigquery.LoadJobConfig(
        schema=schemaBq
       # ,skip_leading_rows=1
    )

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    client = bigquery.Client(project)
    job = client.load_table_from_dataframe(
        dfToBq, dataset+"." + tabla, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    print("Se creo la tabla:"+dataset+"." + tabla)
