# generates ecs tasks for experiment 1
import boto3

ecs_client = boto3.client('ecs', region_name="us-east-2")
cw_client = boto3.client('logs', region_name="us-east-2")


def create_task(experiment_number, numberOfFiles, fileSize, storageAccount, accessKey):
    response = ecs_client.register_task_definition(
        family='StorageAccountWriter%s_%d_%d' % (storageAccount, experiment_number, numberOfFiles),
        executionRoleArn='arn:aws:iam::956882708938:role/ecsTaskExecutionRole',
        networkMode='awsvpc',
        containerDefinitions=[
            {
                'name': 'storageaccountwriter',
                'image': '956882708938.dkr.ecr.us-east-2.amazonaws.com/storageaccountwriter:1.0.0',
                'memoryReservation': 128,
                'portMappings': [],
                'essential': True,
                'environment': [
                    {'name': "AccountName", "value": storageAccount},
                    {'name': "AccessKey", "value": accessKey},
                    {'name': "BlobName", "value":  "blob_from_image.json"},
                    {'name': "ContainerName", "value":  "appendblobexp%d-%d-%d" % (experiment_number, numberOfFiles, fileSize)},
                    {'name': "MaxLogFileSize", "value":  str(fileSize)}
                ],
                'logConfiguration': {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/ecs/StorageAccountWriter%s_%d_%d" % (storageAccount, experiment_number, numberOfFiles),
                        "awslogs-region": "us-east-2",
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            },
        ],
        inferenceAccelerators=[],
        volumes=[],
        requiresCompatibilities=['FARGATE'],
        cpu='256',
        memory='512'
    )
    print(response)
    return response

def run_task(task_definition, blob_name):
    response = ecs_client.run_task(
            cluster='arn:aws:ecs:us-east-2:956882708938:cluster/AzurePerfTesting',
            count=1,
            enableECSManagedTags=True,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        "subnet-3f03f656", "subnet-b88ba4f2", "subnet-04a9a67c"
                    ],
                    'securityGroups': [
                        "sg-0ae3d00960d684538"
                    ],
                    'assignPublicIp': 'ENABLED'
                }
            },
        overrides={
            'containerOverrides': [
                {
                    'name': 'storageaccountwriter',
                    'environment': [
                        {
                            'name': 'BlobName',
                            'value': blob_name
                        },
                    ]
                },
            ]
        },
        taskDefinition=task_definition
    )
    print(response)

def create_cluster():
    pass


def createStorageAccount():
    pass


def startExperiment1():
    storageAccount="allbloblogseastus"
    storageAccountAccessKey="<storage account accessKey>"
    numberOfFiles=5000
    fileSize=100*1024*1024
    experiment_number = 4
    try:
        cw_client.create_log_group(logGroupName="/ecs/StorageAccountWriter%s_%d_%d" % (storageAccount, experiment_number, numberOfFiles))
    except Exception as e:
        print(e)
    response = create_task(experiment_number, numberOfFiles, fileSize, storageAccount, storageAccountAccessKey)
    revision_number = response.get("taskDefinition", {}).get("taskDefinitionArn").rsplit(":", 1)[-1]
    task_definition = 'StorageAccountWriter%s_%d_%d' % (storageAccount, experiment_number, numberOfFiles) + ":" + str(revision_number)
    print("Using revision", revision_number, task_definition)
    for i in range(1, numberOfFiles+1):
        blob_name = 'appendblobfile%s_%d_%d_%d_%d.json' % (storageAccount, experiment_number, numberOfFiles, fileSize, i)
        run_task(task_definition, blob_name)

if __name__ == '__main__':
    startExperiment1()
