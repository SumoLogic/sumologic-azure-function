import os
from datetime import datetime
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, BlobClient


def getLastLogLineNumber(blob_client, current_file_size):
    if current_file_size <= 0:
        return 0
    offset = max(current_file_size-500, 0)
    blob_data = blob_client.download_blob(offset=offset, length=512)
    data = blob_data.content_as_text()
    line_num = data.rsplit("LineNo\":", 1)[-1].rstrip('}\n').strip()
    return int(line_num)


def utf8len(s):
    if not isinstance(s, bytes):
        s = s.encode('utf-8')

    length = len(s)
    del s
    return length

def upload_file_chunks_using_append_blobs():

    '''
        azure-storage-blob==12.5.0
        https://docs.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python
    '''
    # blob_path = "resourceId=/SUBSCRIPTIONS/C088DC46-D692-42AD-A4B6-9A542D28AD2A/RESOURCEGROUPS/SUMOAUDITCOLLECTION/PROVIDERS/MICROSOFT.WEB/SITES/HIMTEST/y=2020/m=11/d=02/h=06/m=00/"

    account_name = os.getenv("AccountName")
    account_access_key = os.getenv("AccessKey")
    blob_name = os.getenv("BlobName")
    container_name = os.getenv("ContainerName")
    max_file_size = int(os.getenv("MaxLogFileSize"))
    blob_service_client = BlobServiceClient(account_url="https://%s.blob.core.windows.net" % account_name, credential=account_access_key)

    container_client = blob_service_client.get_container_client(container_name)
    blob_client = None
    try:
        container_client.create_container()
    except ResourceExistsError:
        print("Container Already Exists")

    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        blob_client.create_append_blob()
        current_file_size = 0
        print(f"Creating new file storage: {account_name} container: {container_name} blob: {blob_name} ")
        log_line_num = 0
    else:
        current_file_size = blob_client.get_blob_properties().size
        log_line_num = getLastLogLineNumber(blob_client, current_file_size)

    print(f"current_file_size (in MB): {current_file_size/(1024*1024)} log_line_num: {log_line_num} storage: {account_name} container: {container_name} blob: {blob_name} ")
    logline = '''{ "time": "TIMESTAMP", "resourceId": "/SUBSCRIPTIONS/C088DC46-D692-42AD-A4B6-9A542D28AD2A/RESOURCEGROUPS/SUMOAUDITCOLLECTION/PROVIDERS/MICROSOFT.WEB/SITES/HIMTEST", "operationName": "Microsoft.Web/sites/log", "category": "AppServiceConsoleLogs", "resultDescription": "000000000 WARNING:root:testing warn level\\n\\n", "level": "Error", "EventStampType": "Stamp", "EventPrimaryStampName": "waws-prod-blu-161", "EventStampName": "waws-prod-blu-161h", "Host": "RD501AC57BA3D4", "LineNo": LINENUM}'''

    while current_file_size < max_file_size:
        # since (4*1024*1024)/512(size of logline) = 8192
        msg = []
        lines_remaining = 8192 - log_line_num
        for idx in range(lines_remaining):
            log_line_num += 1
            current_datetime = datetime.now().isoformat()
            cur_msg = logline.replace("TIMESTAMP", current_datetime)
            cur_msg = cur_msg.replace("LINENUM", f'{log_line_num:10d}')
            msg.append(cur_msg)

        chunk = "\n".join(msg) + "\n"
        cur_size = utf8len(chunk)
        current_file_size += cur_size
        print(f"current_chunk_size (in MB): {cur_size/(1024*1024)} log_line_num: {log_line_num} current_file_size: {current_file_size/(1024*1024)} storage: {account_name} container: {container_name} blob: {blob_name} ")
        blob_client.append_block(chunk)
            # time.sleep(20)

    print(f"Finished uploading current_file_size (in MB): {current_file_size/(1024*1024)} last_log_line_num: {log_line_num} storage: {account_name} container: {container_name} blob: {blob_name} ")


def upload_file_chunks_using_block_blobs():
    pass


if __name__ == '__main__':
    # for testing locally uncomment below code
    # os.environ({
    #     "AccountName": "allbloblogseastus",
    #     "AccessKey": "<storage account access key>",
    #     "BlobName": "blob_1_with_newline.json",
    #     "ContainerName": "testappendblob",
    #     "MaxLogFileSize": str(1*1024*1024)}
    # )
    upload_file_chunks_using_append_blobs()
    # upload_file_chunks_using_block_blobs()

