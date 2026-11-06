FROM mcr.microsoft.com/azure-functions/python:4-python3.12
COPY azure_summary/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY src/dpd_etl /home/site/wwwroot/dpd_etl
COPY azure_summary/function_app.py azure_summary/host.json /home/site/wwwroot/
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true
