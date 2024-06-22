#!/bin/bash


npm run build
rm -r ../target_zip
cp -r ../target ../target_zip
cd ../target_zip



if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi

rm .DS_Store


echo "creating node modules folder"

cd producer_build/BlobTaskProducer
echo "Installing dependencies for producer_build in folder: ",$(pwd)
npm install
if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi
cd ../../




cd consumer_build/BlobTaskConsumer
echo "Installing dependencies for consumer_build in folder: ",$(pwd)
npm install
if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi
cd ../../




cd dlqprocessor_build/DLQTaskConsumer
echo "Installing dependencies for dlqprocessor_build in folder: ",$(pwd)
npm install
if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi
cd ../../




echo "removing packagejson"

# rm producer_build/package-lock.json
# rm consumer_build/package-lock.json
# rm dlqprocessor_build/package-lock.json

# rm producer_build/package.json
# rm consumer_build/package.json
# rm dlqprocessor_build/package.json

if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi

echo "creating zip"
version="4.1.0"
producer_zip_file="taskproducer$version.zip"
consumer_zip_file="taskconsumer$version.zip"
dlqprocessor_zip_file="dlqprocessor$version.zip"

cd producer_build && zip -r "../$producer_zip_file" * && cd ..
cd consumer_build && zip -r "../$consumer_zip_file" * && cd ..
cd dlqprocessor_build && zip -r "../$dlqprocessor_zip_file" * && cd ..

if [ $? -eq 0 ]; then
   echo OK
else
   echo FAIL
   exit 1
fi

export AWS_PROFILE="prod"

echo "uploading zip"
bucket_name="appdev-cloudformation-templates"
folder_prefix="AzureBlobReader/"
bucket_region="us-east-1"

aws s3 cp "$producer_zip_file" "s3://${bucket_name}/${folder_prefix}" --acl public-read --region "${bucket_region}"

aws s3 cp "$consumer_zip_file" "s3://${bucket_name}/${folder_prefix}" --acl public-read --region "${bucket_region}"

aws s3 cp "$dlqprocessor_zip_file" "s3://${bucket_name}/${folder_prefix}" --acl public-read --region "${bucket_region}"


if [ $? -eq 0 ]; then
   echo "zip uploaded successfully!"
else
   echo FAIL
   exit 1
fi
