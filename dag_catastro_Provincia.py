import os
import gc
import logging
from datetime import datetime

# --- IMPORTACIONES LIGERAS ---
# Solo lo absolutamente esencial.
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# --- CONSTANTES ---
# Seguras porque no realizan operaciones.
BQ_PROJECT_ID = "data-sri-462514"
BQ_DATASET_ID = "catastro"
BQ_TABLE_ID = "catastro_ruc"
SRI_DATASETS_URL = 'https://www.sri.gob.ec/datasets'
TEMP_DATA_DIR = "/opt/airflow/temp_data/sri_ruc"
GCP_CONN_ID = "google_cloud_bigquery"

@dag(
    dag_id="sri_ruc_etl_to_bigquery",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=['sri', 'etl', 'bigquery', 'gcs', 'optimized'],
    max_active_tasks=4,
    doc_md="""
    ### DAG de ETL para el Catastro RUC del SRI (v4 - Totalmente basado en TaskFlow)
    Este DAG es ultra-eficiente y sigue las mejores prácticas más estrictas:
    1.  **Lazy Imports**: Todas las librerías pesadas se importan dentro de las tareas.
    2.  **TaskFlow API**: No se usan operadores tradicionales; todo son funciones Python.
    3.  **Hooks**: La interacción con GCP se realiza a través de Hooks, el método recomendado dentro de las tareas Python.
    Esto garantiza un tiempo de análisis del DAG casi instantáneo.
    """
)
def sri_ruc_etl_dag():

    @task(task_id="crear_directorio_temporal")
    def create_temp_dir():
        os.makedirs(TEMP_DATA_DIR, exist_ok=True)

    @task(task_id="obtener_enlaces_descarga")
    def get_download_links() -> list:
        import requests
        from bs4 import BeautifulSoup
        
        response = requests.get(SRI_DATASETS_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        base_url = "https://descargas.sri.gob.ec"
        download_links = [
            (base_url + link['href'] if not link['href'].startswith('http') else link['href'])
            for link in soup.find_all('a', href=True)
            if 'download/datosAbiertos/SRI_RUC_' in link['href']
        ]
        return sorted(list(set(download_links)))

    @task(task_id="descargar_y_transformar_provincia")
    def download_and_transform_province(link: str) -> str:
        import io
        import zipfile
        from urllib.parse import urlparse
        import pandas as pd
        import requests

        provincia_zip_name = os.path.basename(urlparse(link).path)
        provincia = provincia_zip_name.replace('SRI_RUC_', '').replace('.zip', '')
        output_path = os.path.join(TEMP_DATA_DIR, f"{provincia}.parquet")
        
        try:
            response = requests.get(link)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_filename = [name for name in z.namelist() if name.endswith('.csv')][0]
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, sep='|', encoding='latin1', low_memory=False)
            
            column_mapping = {'NUMERO_RUC': 'ruc', 'RAZON_SOCIAL': 'razon_social', 'PROVINCIA_JURISDICCION': 'provincia_jurisdiccion', 'ESTADO_CONTRIBUYENTE': 'estado_contribuyente', 'CLASE_CONTRIBUYENTE': 'clase_contribuyente', 'FECHA_INICIO_ACTIVIDADES': 'fecha_inicio_actividades', 'FECHA_ACTUALIZACION': 'fecha_actualizacion', 'FECHA_SUSPENSION_DEFINITIVA': 'fecha_suspension_definitiva', 'FECHA_REINICIO_ACTIVIDADES': 'fecha_reinicio_actividades', 'OBLIGADO': 'obligado_contabilidad', 'TIPO_CONTRIBUYENTE': 'tipo_contribuyente', 'NUMERO_ESTABLECIMIENTO': 'numero_establecimientos', 'NOMBRE_FANTASIA_COMERCIAL': 'nombre_comercial', 'ESTADO_ESTABLECIMIENTO': 'estado_establecimiento', 'DESCRIPCION_PROVINCIA_EST': 'provincia_establecimiento', 'DESCRIPCION_CANTON_EST': 'canton_establecimiento', 'DESCRIPCION_PARROQUIA_EST': 'parroquia_establecimiento', 'CODIGO_CIIU': 'codigo_ciiu', 'ACTIVIDAD_ECONOMICA': 'actividad_economica', 'AGENTE_RETENCION': 'es_agente_retencion', 'ESPECIAL': 'es_contribuyente_especial'}
            df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)
            binary_cols = ['obligado_contabilidad', 'es_agente_retencion', 'es_contribuyente_especial']
            for col in df.columns:
                if col in binary_cols and col in df: df[col] = (df[col].astype(str).str.strip().str.upper() == 'S').astype('int8')
                elif df[col].dtype == 'object':
                    df[col] = df[col].fillna('NO ESPECIFICADO').astype(str).str.strip().str.upper()
                    if len(df[col].unique()) / len(df[col]) < 0.5: df[col] = df[col].astype('category')
            for col in [c for c in df.columns if 'fecha' in c]:
                if col in df: df[col] = pd.to_datetime(df[col], errors='coerce')

            df.to_parquet(output_path, index=False)
            del df
            gc.collect()
            return output_path
        except Exception as e:
            logging.error(f"Error al procesar {provincia}: {e}", exc_info=True)
            return "ERROR"

    @task(task_id="consolidar_y_subir_a_gcs")
    def consolidate_and_upload_to_gcs(file_paths: list) -> str:
        import pandas as pd
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        from airflow.models.variable import Variable

        valid_paths = [path for path in file_paths if path != "ERROR"]
        if not valid_paths: raise ValueError("No se procesó ningún archivo con éxito.")
        
        df_list = [pd.read_parquet(path) for path in valid_paths]
        final_df = pd.concat(df_list, ignore_index=True)
        
        consolidated_filename = "catastro_ruc_consolidado.parquet"
        local_path = os.path.join(TEMP_DATA_DIR, consolidated_filename)
        final_df.to_parquet(local_path, index=False)
        
        bucket_name = Variable.get("gcs_sri_etl_bucket")
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        gcs_object_name = f"sri_data/{consolidated_filename}"
        
        gcs_hook.upload(bucket_name=bucket_name, object_name=gcs_object_name, filename=local_path)

        for path in valid_paths: os.remove(path)
        os.remove(local_path)
        return gcs_object_name

    @task(task_id="cargar_gcs_a_bigquery")
    def load_gcs_to_bigquery(gcs_object_path: str):
        """Carga un archivo desde GCS a BigQuery usando un Hook."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.models.variable import Variable

        bucket_name = Variable.get("gcs_sri_etl_bucket")
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        
        logging.info(f"Cargando gs://{bucket_name}/{gcs_object_path} en BigQuery...")
        
        hook.run_load(
            destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}",
            source_uris=[f"gs://{bucket_name}/{gcs_object_path}"],
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

    @task(task_id="limpiar_staging_en_gcs")
    def cleanup_gcs_task(gcs_object_path: str):
        """Elimina el archivo de staging de GCS usando un Hook."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        from airflow.models.variable import Variable

        bucket_name = Variable.get("gcs_sri_etl_bucket")
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        
        logging.info(f"Eliminando gs://{bucket_name}/{gcs_object_path}...")
        hook.delete(bucket_name=bucket_name, objects=[gcs_object_path])

    # --- Flujo del DAG ---
    start = create_temp_dir()
    links = get_download_links()
    processed_files = download_and_transform_province.expand(link=links)
    gcs_file_path = consolidate_and_upload_to_gcs(file_paths=processed_files)
    # La tarea de carga a BQ ahora es una función Python
    load_task = load_gcs_to_bigquery(gcs_object_path=gcs_file_path)
    # La tarea de limpieza también es una función Python
    cleanup_task = cleanup_gcs_task(gcs_object_path=gcs_file_path)
    end = EmptyOperator(task_id="fin")

    start >> links >> processed_files >> gcs_file_path >> load_task >> cleanup_task >> end

# Instanciar el DAG
sri_ruc_etl_dag()