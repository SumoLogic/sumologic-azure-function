FROM python:3.8-alpine as base

#https://stackoverflow.com/questions/66118337/how-to-get-rid-of-cryptography-build-error

# this is needed to compile psutil and any other packages which require build from source
RUN apk add --no-cache --virtual .build-deps gcc musl-dev linux-headers python3-dev libffi-dev cargo openssl-dev

# note to always change version otherwise docker may use cache
RUN pip install --user azure-storage-blob==12.5.0

RUN apk del .build-deps

# copy deps to new image
FROM python:3.8-alpine AS build-image
COPY --from=base /root/.local /root/.local

# Make sure scripts in .local are usable:
ENV PATH=/root/.local/bin:$PATH

COPY storageaccountwriter/storageaccountwriter.py .
# command to run on container start
CMD [ "python", "storageaccountwriter.py"]
