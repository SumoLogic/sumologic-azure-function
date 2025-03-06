# from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential
import json
import os
from azure.mgmt.resource import ResourceManagementClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from sumologic import SumoLogic

config = {
    "AZURE_SUBSCRIPTION_ID": os.environ.get("AZURE_SUBSCRIPTION_ID"),
    "AZURE_CLIENT_ID": os.environ.get("AZURE_CLIENT_ID"),
    "AZURE_CLIENT_SECRET": os.environ.get("AZURE_CLIENT_SECRET"),
    "AZURE_TENANT_ID": os.environ.get("AZURE_TENANT_ID"),
    "AZURE_DEFAULT_REGION": os.environ.get("AZURE_DEFAULT_REGION"),
    "SUMO_ACCESS_ID": os.environ.get("SUMO_ACCESS_ID"),
    "SUMO_ACCESS_KEY": os.environ.get("SUMO_ACCESS_KEY"),
    "SUMO_DEPLOYMENT": os.environ.get("SUMO_DEPLOYMENT")
}

# config_file = os.path.expanduser("~/.azure/azure_credentials.json")
# config = json.load(open(config_file))
subscription_id = str(config['AZURE_SUBSCRIPTION_ID'])
credentials = ClientSecretCredential(
    client_id=config['AZURE_CLIENT_ID'],
    client_secret=config['AZURE_CLIENT_SECRET'],
    tenant_id=config['AZURE_TENANT_ID']
)
location = str(config['AZURE_DEFAULT_REGION'])

print("creating credentials", subscription_id)

resource_client = ResourceManagementClient(credentials, subscription_id)


def delete_resource_group(resource_group_name):
    print("Found %s " % resource_group_name)
    resp = resource_client.resource_groups.begin_delete(resource_group_name)
    resp.wait()
    print('Deleted {}'.format(resource_group_name), resp.status())


groups = resource_client.resource_groups.list()
future_to_group = {}
with ThreadPoolExecutor(max_workers=10) as executor:
    for group in groups:
        group_name = group.name
        if (group_name.startswith("TBL") or group_name.startswith("TABR") or group_name.startswith("testsumosa")):
            print(f"scheduling {group_name}")
            future_to_group[executor.submit(delete_resource_group, group_name)] = group_name

if future_to_group:
    for future in as_completed(future_to_group):
        group_name = future_to_group[future]
        try:
            future.result()
            print(f"Task completed for {group_name}")
        except Exception as exc:
            print('%r generated an exception: %s' % (group_name, exc))
else:
    print("No resource group found")


def api_endpoint(sumo_deployment):
    if sumo_deployment == "us1":
        return "https://api.sumologic.com/api"
    elif sumo_deployment in ["ca", "au", "de", "eu", "jp", "us2", "fed", "in"]:
        return "https://api.%s.sumologic.com/api" % sumo_deployment
    else:
        return 'https://%s-api.sumologic.net/api' % sumo_deployment

sumologic_cli = SumoLogic(config["SUMO_ACCESS_ID"], config["SUMO_ACCESS_KEY"], api_endpoint(config["SUMO_DEPLOYMENT"]))


def delete_collector(collector_id, collector_name):
    print("Found %s " % collector_name)
    resp = sumologic_cli.delete_collector({"collector": {"id": collector_id}})
    print('Deleted {}'.format(collector_name), resp)


future_to_group = {}
offset = 0
with ThreadPoolExecutor(max_workers=5) as executor:
    while True:
        collectors = sumologic_cli.collectors(limit=1000, offset=offset, filter_type="hosted")
        if not collectors:
            break
        for collector in collectors:
            collector_name = collector["name"]
            collector_id = collector["id"]
            if (collector_name.startswith("azure_appendblob_unittest") or collector_name.startswith("azure_blockblob_unittest")):
                print(f"scheduling {collector_name}")
                future_to_group[executor.submit(delete_collector, collector_id, collector_name)] = collector_name
        offset += 100


if future_to_group:
    for future in as_completed(future_to_group):
        collector_name = future_to_group[future]
        try:
            future.result()
            print(f"Task completed for {collector_name}")
        except Exception as exc:
            print('%r generated an exception: %s' % (collector_name, exc))
else:
    print("No collector found")






