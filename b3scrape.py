import os
import json
import base64
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env (certifique-se de que o arquivo esteja na raiz do projeto)
load_dotenv()

import boto3

class IBOVDataExtractor:
    URL_B3 = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
    DEFAULT_PAYLOAD = {"language": "pt-br", "pageNumber": 1, "pageSize": 20, "index": "IBOV", "segment": "1"}
    
    def __init__(self, download_dir="download", sink_dir=None):
        # Parâmetros locais (caso queira também salvar localmente, se necessário)
        self.download_dir = download_dir
        self.sink_dir = sink_dir or f"./{download_dir}"
        os.makedirs(self.sink_dir, exist_ok=True)
        self.today_date = datetime.today().strftime('%d-%m-%y')
        self.payload = self.DEFAULT_PAYLOAD.copy()
        
        # Carrega variáveis AWS do .env
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')  # Se houver
        self.S3_BUCKET = os.getenv('S3_BUCKET')  # Ex: 'meu-bucket'
        self.S3_BUCKET_PATH = os.getenv('S3_BUCKET_PATH')  # Ex: 'raw/'
        
        # Inicializa o cliente S3 com as credenciais fornecidas
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            aws_session_token=self.AWS_SESSION_TOKEN
        )
    
    def encode_payload(self, payload):
        payload_json = json.dumps(payload)
        return base64.b64encode(payload_json.encode("utf-8")).decode("utf-8")
    
    def fetch_page(self, payload):
        encoded_payload = self.encode_payload(payload)
        url = self.URL_B3 + encoded_payload
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f"Request for page {payload.get('pageNumber')} failed with status code {response.status_code}"
            )
        return response.json()
    
    def get_total_pages(self, initial_data):
        return abs(initial_data.get("page", {}).get("totalPages", 1))
    
    def fetch_all_pages(self):
        # Inicia pela primeira página para determinar o total de páginas
        initial_data = self.fetch_page(self.payload)
        total_pages = self.get_total_pages(initial_data)
        all_results = initial_data.get("results", [])
        print(f"Total pages found: {total_pages}")
        
        for page in range(2, total_pages + 1):
            self.payload["pageNumber"] = page
            try:
                page_data = self.fetch_page(self.payload)
                results = page_data.get("results", [])
                all_results.extend(results)
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                continue
        return all_results
    
    def consolidate_results(self, results):
        df = pd.DataFrame(results)
        if df.empty:
            print("No data retrieved.")
        return df
    
    def add_date_partition(self, df):
        today = datetime.today().strftime("%Y-%m-%d")
        df["date"] = today
        return df
    
    def save_as_parquet(self, df, partition_col="date"):
        # Define o nome do arquivo usando a partição de data
        partition_value = df[partition_col].iloc[0]
        filename = f"carteira-dia-{partition_value}.parquet"
        # Cria uma tabela do pyarrow a partir do DataFrame
        table = pa.Table.from_pandas(df)
        
        # Salva a tabela em um buffer de memória
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)  # Retorna ao início do buffer
        
        # Envia o arquivo para o bucket S3 na pasta especificada
        s3_key = f"{self.S3_BUCKET_PATH}{self.today_date}/{filename}"
        self.s3_client.put_object(
            Bucket=self.S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        print(f"Data saved to S3: s3://{self.S3_BUCKET}/{s3_key}/{self.today_date}")
    
    def process(self):
        print("Starting data extraction process...")
        results = self.fetch_all_pages()
        df = self.consolidate_results(results)
        if df.empty:
            return
        df = self.add_date_partition(df)

        # casting df cols
        df['part'] = df['part'].str.replace(',', '.').astype(float)
        df['theoricalQty'] = df['theoricalQty'].str.replace('.', '', regex=False).astype(int)
        #=================

        print("Combined DataFrame shape:", df.shape)
        print(df.head())
        self.save_as_parquet(df)
        print("Processing complete.")

if __name__ == "__main__":
    extractor = IBOVDataExtractor()
    extractor.process()