from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import sharepy
import urllib3
import requests
from io import BytesIO
urllib3.disable_warnings()

# SharePoint site and file details
# site_url = "https://bosch.sharepoint.com/sites/msteams_68b5c69d-8.Batch_2/"
# relative_file_url = "/sites/msteams_68b5c69d-8.Batch_2/Shared Documents/8.Batch_2/Assignments/sale_data.csv"
# SharePoint site and file details
site_url = "https://bosch-my.sharepoint.com/personal/hav5hc_bosch_com"
# relative_file_url = "/personal/hav5hc_bosch_com/Documents/sale_data_vi.csv"
relative_file_url = "/personal/hav5hc_bosch_com/Documents/Documents/sale_data_with_cost_profit.csv"
# User credentials
username = ""  # Replace with your SharePoint username
password = ""  # Replace with your SharePoint password
                        
ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
file_content = BytesIO()
ctx.web.get_file_by_server_relative_url(relative_file_url).download(file_content).execute_query()
# Convert bytes to string (assuming UTF-8 encoding)
file_text = file_content.getvalue().decode("utf-8")  # Adjust encoding if needed
print(file_text)  # Print or process file content


