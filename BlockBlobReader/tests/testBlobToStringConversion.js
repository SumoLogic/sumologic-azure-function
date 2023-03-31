const fs = require("fs");
const { AbortController } = require("@azure/abort-controller");


const { ContainerClient, BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");

async function main() {
  
  const account = "storageAccountName";
  const accountKey = "storageAccountKey";
  

  const sharedKeyCredential = new StorageSharedKeyCredential(account, accountKey);

  const blobServiceClient = new BlobServiceClient(
    `https://${account}.blob.core.windows.net`,
    sharedKeyCredential
  );

  const containerClient = new ContainerClient(
    `https://${account}.blob.core.windows.net/csu3`,
    sharedKeyCredential
  );

  const blockBlobClient = containerClient.getBlockBlobClient("blob_fixtures.blob");

  // console.log("Containers:");
  // for await (const container of blobServiceClient.listContainers()) {
  //   console.log(`- ${container.name}`);
  // }

  // console.log("Blobs in csu3:");
  // for await (const blob of containerClient.listBlobsFlat()) {
  //   console.log(`- ${blob.name}`);
  // }

  // const fileSize = fs.statSync("blob_fixtures.blob").size;
  // console.log(fileSize)
  const buffer = Buffer.alloc(4 * 1024 * 1024);
  try {
    await blockBlobClient.downloadToBuffer(buffer, 0, 200, {
      abortSignal: AbortController.timeout(30 * 60 * 1000),
      blockSize: 4 * 1024 * 1024,
      concurrency: 20,
      onProgress: (ev) => console.log(ev),
    });
    // console.log("downloadToBuffer succeeds");
  } catch (err) {
    console.log(err);
  }

  console.log(buffer.toString());

}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});