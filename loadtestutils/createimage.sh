#!/usr/bin/env bash


export AWS_PROFILE=prod
version="1.0.0"
image_name="storageaccountwriter"
account_id="956882708938"
region="us-east-2"
image_tag="$image_name:$version"

docker build --tag $image_tag -f "Dockerfile" .

docker image ls "$image_name"


docker tag $image_tag $account_id.dkr.ecr.$region.amazonaws.com/$image_tag

aws ecr get-login-password --region $region | docker login --username AWS --password-stdin $account_id.dkr.ecr.$region.amazonaws.com

docker push $account_id.dkr.ecr.$region.amazonaws.com/$image_tag


