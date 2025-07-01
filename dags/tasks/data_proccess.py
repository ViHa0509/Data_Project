import io
import os
import sharepy
import pandas as pd 
from sqlalchemy import create_engine
from airflow.models import Variable
import urllib3 
import json
from tasks.sale_data_stg import stg_shema_generate
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

urllib3.disable_warnings()

def read_exec_data_stg():
    #URL = "https://bosch.sharepoint.com/sites/msteams_68b5c69d-8.Batch_2/"
    #FILE_URL = "/sites/msteams_68b5c69d-8.Batch_2/Shared Documents/8.Batch_2/Assignments/sale_data.csv"
    # URL = "https://bosch-my.sharepoint.com/personal/hav5hc_bosch_com/"
    # FILE_URL = "/personal/hav5hc_bosch_com/Documents/sale_data_vi.csv"
    URL = "https://bosch-my.sharepoint.com/personal/hav5hc_bosch_com/"
    FILE_URL = "/personal/hav5hc_bosch_com/Documents/Documents/sale_data_with_cost_profit.csv"

    SP_USERNAME = Variable.get("hav5hc_username")
    SP_PASSWORD = Variable.get("hav5hc_password")
    print(f"username: {SP_USERNAME}")

    # Authenticate with SharePoint
    credentials = UserCredential(SP_USERNAME, SP_PASSWORD)
    ctx = ClientContext(URL).with_credentials(credentials)
    try:
        response = File.open_binary(ctx, FILE_URL)
        # Decode response content
        decoded_content = response.content.decode('utf-8')
        print(decoded_content)
        # Read into DataFrame
        df = pd.read_csv(io.StringIO(decoded_content))
        # Decode response content
        csv_buffer = io.StringIO() 
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        stg_shema_generate(csv_buffer)

    except Exception as e:
        print(f"An error occurred: {e}")
