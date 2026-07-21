import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

def connect_postgres():
    return create_engine('String_de_conexao')


def get_dataset_metadata(dataset_id):
    
    url = (
        f"https://dados.ons.org.br/api/3/action/package_show"
        f"?id={dataset_id}"
    )

    response = requests.get(url)
    response.raise_for_status()

    return response.json()["result"]

def get_resources_metadata():
    datasets = ["8b828682-fe7d-4aa1-9e5e-f5aeb1ebb92e",
            "202ceb44-79ea-47bd-be2a-602410249ea9",
            "c712b4ed-08fd-4c79-b746-e400ed480dd1",
            "3598f99c-af24-468d-a8ee-1bbb9816cc34",
            "209b9d9f-a1ce-4a55-9f01-31628f26257f"]

    resources = []

    conn = connect_postgres()

    for id in datasets:

        ds_metadata = get_dataset_metadata(id)

        for resource in ds_metadata["resources"]:

            if resource.get("format").lower() == "csv":

                resources.append([ds_metadata["id"],
                                  ds_metadata["name"],
                                resource["id"],
                                resource["name"],
                                resource["url"],
                                resource["format"],
                                resource["last_modified"]])

    df_resources = pd.DataFrame(resources,
                                columns = ["dataset_id",
                                           "dataset_name",
                                        "resource_id",
                                        "resource_name",
                                        "resource_url",
                                        "resource_format",
                                        "last_modified"])
    
    df_resources["reference_date"] = df_resources["resource_name"].str.extract(r"(\d{4})").astype(int)

    try:
        df_resources.to_sql(name = "resource_status",
                            con = conn,
                            schema = "intercambio_internacional",
                            if_exists="fail",
                            index = False)
    except:
        return True
    
@task(task_id = "criar_tabela_descricao")
def criar_tabela_resource():

    with connect_postgres().connect() as conn:

        conn.execute(text("CREATE SCHEMA IF NOT EXISTS intercambio_internacional;"))

        conn.commit()

    get_resources_metadata()

    return True


@task(task_id = "extrair_dados")
def extract(dataset_name):

    cn = connect_postgres()

    df = pd.read_sql(f"""SELECT * FROM postgres.intercambio_internacional.resource_status WHERE dataset_name = '{dataset_name}' """,con = cn)
    
    resource = pd.DataFrame()

    for _, row in df.iterrows():

        resource = pd.concat([resource,
                              pd.read_csv(
                                  StringIO(requests.get(row["resource_url"]).text)
                                  , sep = ';')])

    return resource

@task(task_id = "carregar_dados")
def load(table_name, resource_table):

    cn = connect_postgres()
    
    resource_table.to_sql(name = table_name,
                    con = cn,
                    schema = "intercambio_internacional",
                    if_exists = "replace",
                    index = False
                    )
    
with DAG(dag_id = "dados_intercambio_internacional_ons",
         start_date = datetime(2026, 1, 1),
         schedule = "@daily",
         catchup = False) as dag:
    
    inicio = EmptyOperator(task_id = "inicio")
    consolidacao_final = EmptyOperator(task_id="consolidar_balanco_energetico")
    checar_dados = criar_tabela_resource()


    table_names = {
        'intercambio_modalidade': 'tb_modalidade',
        'intercambio-internacional': 'tb_intercambio',
        'geracao-exportacao-internacional': 'tb_exptermicas',
        'ofertapreco-importacao': 'tb_ofertapreco',
        'importacaoenergia-comercial-2': 'tb_blocoimportacao'
    }
    
    for dataset_name, table in table_names.items():

        with TaskGroup(group_id = f"tg_{dataset_name.replace("-","_")}") as tg:

            extracted_data = extract(dataset_name= dataset_name)

            load_data = load(table_name= table, resource_table= extracted_data)

            extracted_data >> load_data

        inicio >> checar_dados >> tg >> consolidacao_final