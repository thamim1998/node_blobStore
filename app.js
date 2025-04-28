const express = require('express');
const { BlobServiceClient } = require("@azure/storage-blob");
require("dotenv").config();

const app = express();
app.use(express.json());

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const containerName = process.env.AZURE_STORAGE_CONTAINER_NAME;

if (!connectionString || !containerName) {
  throw new Error("Azure Storage connection string and container name must be set in environment variables");
}

const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
const containerClient = blobServiceClient.getContainerClient(containerName);

// Utility function
async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => chunks.push(data.toString()));
    readableStream.on("end", () => resolve(chunks.join("")));
    readableStream.on("error", reject);
  });
}

// Route handlers
app.post('/upload', async (req, res) => {
  try {
    const { blobName, content } = req.body;
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    await blockBlobClient.upload(content, content.length);
    res.status(200).json({ message: `Uploaded blob: ${blobName}` });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/blobs', async (req, res) => {
  try {
    const blobs = [];
    for await (const blob of containerClient.listBlobsFlat()) {
      blobs.push(blob.name);
    }
    res.status(200).json(blobs);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/download/:blobName', async (req, res) => {
  try {
    const { blobName } = req.params;
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    const downloadResponse = await blockBlobClient.download();
    const content = await streamToString(downloadResponse.readableStreamBody);
    res.status(200).json({ content });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

const port = process.env.PORT || 8080;
app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});