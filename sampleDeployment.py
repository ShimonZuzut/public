import logging
import pyodbc
import pymssql
import os
import json
import os.path
import azure.functions as func

from datetime import datetime
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode
from azure.mgmt.resource.resources.models import Deployment
from azure.mgmt.resource.resources.models import DeploymentProperties

from azure.storage.blob import (
    BlobClient, BlobServiceClient, ContainerClient, PublicAccess)

###### SUB FUNCTION: deploy service ######

def deployService(req_body):

    # Load the parameter reference file as JSON object
    with open(full_path_to_parameter_reference_file, 'r') as parameter_reference_file_fd:
        reference_parameter_object= json.load(parameter_reference_file_fd)

    ## Comparing the input JSON with the reference parameter JSON for the service

    service = req_body.get("service")
    logging.info(f'service = {service}')

    if(len(service) == 0 or service.isspace()):
        # throw error
        logging.info(f'Input parameter "service" not provided.')

    # Remove the 'service' element from requrest body JSON so we can compare it with the reference parameter JSON
    req_body.pop('service')

    reference_json = reference_parameter_object["service"][service]
    logging.info(f'Equal or NOT = {reference_json == req_body}')

    if(reference_json != req_body):
        logging.info(f'Input parameter JSON is missing some elements. Expected sample JSON: {reference_json}. Received JSON: {req_body}')

    logging.info(f'Expected sample JSON: {reference_json}. Received JSON: {req_body}')

    # Construct the service level properties / parameters
    if(service == 'storageaccount'):
        resourcegroup = req_body.get('resourcegroup')
        businessunit = req_body.get('businessunit')
        location = req_body.get('location')
        instancename = req_body.get('instancename')

        # Make sure the parameters are not null
        if resourcegroup is None or businessunit is None or location is None or instancename is None:
            response_message = f'Please pass valid input on the query string or in the request body. Received Inputs -> ResourceGroup:"{resourcegroup}" BusinessUnit:"{businessunit}" Location:"{location}" InstanceName:"{instancename}".'
            return func.HttpResponse(
                response_message,
                status_code=400
            )

    # Calculate the storage account name based on the requrest parameters
    storage_account_to_be_created = businessunit + location + instancename

    # Load the ARM template file as JSON object
    with open(full_path_to_template_file, 'r') as template_file_fd:
        template = json.load(template_file_fd)

    # Construct parameters object
    parameters = {
        'storageAccountName': storage_account_to_be_created
    }
    parameters = {k: {'value': v} for k, v in parameters.items()}

    logging.info(f'Parameters details: {parameters}')
    logging.info(f'Template details; {template}')

    deployment_properties = DeploymentProperties(mode=DeploymentMode.incremental, template=template, parameters=parameters)
    logging.info(f'Deployment properties: {deployment_properties}')

    deploymentdate = datetime.now()
    deploymentname = service + deploymentdate.strftime("%Y%m%d%H%M%S")
    logging.info(f'deployment name = {deploymentname}')

    # Kick off ARM deployment
    deployment_async_operation = client.deployments.create_or_update(
        resourcegroup,
        deploymentname,
        Deployment(properties=deployment_properties)
    )

    # Construct values to pass to 'writeDeploymentDetailsToCMDB' function
    client_subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

    # Deployment status is calcualted using different Azure function and update the CMDB
    deploymentstatus = " "

    logging.info(f'service type = type({service})')
    logging.info(f'resourcegroup type = type({resourcegroup})')
    logging.info(f'location type = type({location})')
    logging.info(f'businessunit type = type({businessunit})')
    logging.info(f'instancename type = type({instancename})')
    logging.info(f'client_subscription_id type = type({client_subscription_id})')
    logging.info(f'deploymentname type = type({deploymentname})')
    logging.info(f'deploymentdate type = type({deploymentdate})')
    logging.info(f'deploymentstatus type = type({deploymentstatus})')
    if(service == 'storageaccount'):
        resourceid = "/subscriptions/" + str(client_subscription_id) + "/resourceGroups/" + str(resourcegroup) + "/providers/Microsoft.Storage/storageAccounts/" + str(storage_account_to_be_created)

    logging.info(f'Resource ID = {resourceid}')

    # Write deployment details into CMDB
    writeDeploymentDetailsToCMDB(service, resourcegroup, location, businessunit, instancename, resourceid, deploymentname, deploymentdate, deploymentstatus)

    logging.info(f'Deployment Async operation: {deployment_async_operation}')

    return str(storage_account_to_be_created)

####### SUB FUNCTION: Write deployment details into CMDB #######

def writeDeploymentDetailsToCMDB(service, resourcegroup, location, businessunit, instancename, resourceid, deploymentname, deploymentdate, deploymentstatus):
    cmdb_server = os.environ["CMDB_SERVERNAME"]
    cmdb_database = os.environ["CMDB_DATABASENAME"]
    cmdb_username = os.environ["CMDB_USERNAME"]
    cmdb_password = os.environ["CMDB_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    query = f'INSERT INTO tblDeploymentDetails VALUES (\'{service}\',\'{resourcegroup}\',\'{location}\',\'{businessunit}\',\'{instancename}\',\'{resourceid}\',\'{deploymentname}\',\'{deploymentdate}\',\'{deploymentstatus}\')'
    logging.info(f'Query = {query}')

    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+cmdb_server+';PORT=1433;DATABASE='+cmdb_database+';UID='+cmdb_username+';PWD='+ cmdb_password)
    cursor = cnxn.cursor()
    cursor.execute(query)
    cnxn.commit()
    """
    with pyodbc.connect('DRIVER='+driver+';SERVER='+cmdb_server+';PORT=1433;DATABASE='+cmdb_database+';UID='+cmdb_username+';PWD='+ cmdb_password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            while row:
                print (str(row[0]) + " " + str(row[1]))
                row = cursor.fetchone()
    """

####### SUB FUNCTION: Authenticate to Azure #######

def authenticateToAzure():

    # Retrieve the IDs and secret to use with ServicePrincipalCredentials
    logging.info('Logging environment Variables...')
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    client_subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    client_tenant_id = os.environ["AZURE_TENANT_ID"]
    global conn_str
    conn_str = os.environ["AZURE_STORAGE_CONNECTIONSTRING"]

    # Create Azure credential object
    credential = ServicePrincipalCredentials(tenant=client_tenant_id, client_id=client_id, secret=client_secret)
    global client
    client = ResourceManagementClient(credential, client_subscription_id)

####### SUB FUNCTION: Download the files from azure storage blob #######

def downloadARMTemplateFromStorageBlob():
    #return func.HttpResponse(f'Client ID: {client_id}')
    container_name = "stockarmtemplates"
    template_blob_name = "storageAccountDeploy.json"
    parameter_blob_name = "storageAccountDeploy.parameters.json"
    parameter_reference_blob_name = "parameterReference.json"

    # Create reference to Storage Account blob using sas token
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Create blob client object -> templateFile
    template_blob_client = blob_service_client.get_blob_client(container=container_name, blob=template_blob_name)

    # Create blob client object -> parameterFile
    parameter_blob_client = blob_service_client.get_blob_client(container=container_name, blob=parameter_blob_name)

    # Create blob client object -> parameterReferenceFile
    parameter_reference_blob_client = blob_service_client.get_blob_client(container=container_name, blob=parameter_reference_blob_name)

    # Create a folder to store the ARM template Files
    local_path = os.path.expanduser("~/temparmtemplates")
    logging.info(f'local path: {local_path}')
    local_path_exists = os.path.exists(local_path)
    logging.info(f'local path exists: {local_path_exists}')

    if not local_path_exists:
        os.makedirs(local_path)

    local_path_exists = os.path.exists(local_path)
    logging.info(f'local path exists again: {local_path_exists}')

    # Construct file path for ARM template file
    global full_path_to_template_file
    full_path_to_template_file = os.path.join(local_path,template_blob_name)
    logging.info(f'Template Ful path to file: {full_path_to_template_file}')

    # Construct file path for ARM parameter file
    global full_path_to_parameter_file
    full_path_to_parameter_file = os.path.join(local_path,parameter_blob_name)
    logging.info(f'Template Ful path to file: {full_path_to_parameter_file}')

    # Construct file path for ARM parameter Reference file
    global full_path_to_parameter_reference_file
    full_path_to_parameter_reference_file = os.path.join(local_path,parameter_reference_blob_name)
    logging.info(f'Template Ful path to file: {full_path_to_parameter_reference_file}')

    # Download ARM Template file blob
    with open(full_path_to_template_file, "wb") as my_blob:
        my_blob.writelines([template_blob_client.download_blob().readall()])

    # Download ARM Parameter file blob
    with open(full_path_to_parameter_file, "wb") as my_blob:
        my_blob.writelines([parameter_blob_client.download_blob().readall()])

    # Download ARM Parameter file blob
    with open(full_path_to_parameter_reference_file, "wb") as my_blob:
        my_blob.writelines([parameter_reference_blob_client.download_blob().readall()])

####### MAIN FUNCTION #######

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass

    # Validating inputs. The input object is a JSON dictionary.
    # Check if any value in the JSON is empty or whitespaces.
    # The SUB functions will check for required input properties based on the service
    for key in req_body:
        if(len(req_body[key]) == 0 or req_body[key].isspace()):
            response_message = f'Please pass valid input on the query string or in the request body. Received Inputs -> ResourceGroup:"{resourcegroup}" BusinessUnit:"{businessunit}" Location:"{location}" InstanceName:"{instancename}".'
            return func.HttpResponse(
             response_message,
             status_code=400
        )

    # Function call for Azure authentication via service principal
    authenticateToAzure()

    logging.info(f' input provided: {req_body}')
    #return func.HttpResponse(f'Deployment via arm template successfully initiated for Storage account: {storage_account_to_be_created}')

    # Function call for downloading ARM template from Storage Blob
    downloadARMTemplateFromStorageBlob()

    # Call the sub function to deploy service
    servicename = " "
    serviceName = deployService(req_body)

    return func.HttpResponse(f'Initiated deployment for the service: {servicename}')
