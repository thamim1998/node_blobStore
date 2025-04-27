const { BlobServiceClient } = require("@azure/storage-blob");
require("dotenv").config();

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const containerName = process.env.AZURE_STORAGE_CONTAINER_NAME;

const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
const containerClient = blobServiceClient.getContainerClient(containerName);

async function uploadBlob(blobName, content) {
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  await blockBlobClient.upload(content, content.length);
  console.log(`Uploaded blob: ${blobName}`);
}

async function listBlobs() {
  console.log("Listing blobs...");
  for await (const blob of containerClient.listBlobsFlat()) {
    console.log(`- ${blob.name}`);
  }
}

async function downloadBlob(blobName) {
  const location = "I:\\test.txt";
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  blockBlobClient.getProperties();
  const downloadResponse = await blockBlobClient.download();
  const content = await streamToString(downloadResponse.readableStreamBody);

  // const downloadResponse = await blockBlobClient.downloadToFile(location);
  console.log(`Downloaded blob "${blobName}":`, content);
}

async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => chunks.push(data.toString()));
    readableStream.on("end", () => resolve(chunks.join("")));
    readableStream.on("error", reject);
  });
}

//to create container
async function createContainer() {
  try {
    const newContainer = "data-node";

    // 1. Create BlobServiceClient
    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);

    // 2. Get a reference to the container
    const containerClient = blobServiceClient.getContainerClient(newContainer);

    // 3. Create the container (if it doesn't exist)
    const createResponse = await containerClient.createIfNotExists();

    if (createResponse.succeeded) {
      console.log(`✅ Container "${newContainer}" created successfully.`);
    } else {
      console.log(`ℹ️ Container "${newContainer}" already exists.`);
    }
  } catch (error) {
    console.error("❌ Error creating container:", error.message);
  }
}

async function getProperties(blobName) {
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  const blobProperties = await blockBlobClient.getProperties();
  console.log(blobProperties.accessTier);
}

async function getMetaData(blobName) {
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  const blobProperties = await blockBlobClient.getProperties();
  const data = blobProperties.metadata;
  for(let [key, value] of Object.entries(data) ) {
    console.log(key, value);
  } 
}

async function setMetaData(blobName) {
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    // 1. Get existing metadata (if any)
    const blobProperties = await blockBlobClient.getProperties();
    const metadata = blobProperties.metadata || {}; // Initialize if undefined

    // 2. Add/Update metadata
     metadata["Tier"] = "1";

    // 3. Apply the updated metadata
    await blockBlobClient.setMetadata(metadata);
    console.log(`✅ Metadata updated for blob: ${blobName}`);
  } catch (error) {
    console.error(`❌ Failed to update metadata for ${blobName}:`, error.message);
  }
}

(async () => {
  // await uploadBlob('test.txt', 'Hello, Azure Blob Storage!');
  // await listBlobs();
  // await downloadBlob('test.txt');
  // await createContainer();
  // await getProperties("test.txt");
  await setMetaData("test.txt");
})();