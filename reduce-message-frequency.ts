import fs from "fs/promises";
import { McapWriter, McapIndexedReader } from "@mcap/core";
import { FileHandleReadable, FileHandleWritable } from "@mcap/nodejs";
import { loadDecompressHandlers } from "@mcap/support";
import zstd from "@lichtblick/wasm-zstd";
import path from "path";

// Read the input file from the command line argument
const inputFiles = process.argv.slice(2);

if (inputFiles.length !== 1) {
  console.error("Usage: npm run reduce <input-file>");
  process.exit(1);
}

reduceFrequency(inputFiles[0]).catch(console.error);

async function reduceFrequency(inputFile) {
  const inputStream = await fs.open(inputFile, "r");
  const reader = await McapIndexedReader.Initialize({
    readable: new FileHandleReadable(inputStream),
    decompressHandlers: await loadDecompressHandlers(),
  });

  const outputFile = path.join(
    process.cwd(),
    path.basename(inputFile).replace(".mcap", "_reduced.mcap")
  );
  const outputStream = await fs.open(outputFile, "w");
  const writer = new McapWriter({
    writable: new FileHandleWritable(outputStream),
    compressChunk: (data) => ({
      compression: "zstd",
      compressedData: zstd.compress(data),
    }),
  });

  await writer.start({ library: "json", profile: "mcap-point-cloud" });

  const schemaMap = new Map();
  const channelMap = new Map();

  const topicsWriten = {};

  // Copy schemas
  for (const schema of reader.schemasById.values()) {
    const newSchemaId = await writer.registerSchema(schema);
    schemaMap.set(schema.id, newSchemaId);
  }

  // Copy channels
  for (const channel of reader.channelsById.values()) {
    const newSchemaId = schemaMap.get(channel.schemaId) || 0;
    const newChannelId = await writer.registerChannel({
      ...channel,
      schemaId: newSchemaId,
    });
    channelMap.set(channel.id, newChannelId);
  }

  // Copy messages
  for await (const record of reader.readMessages()) {
    if (record.type !== "Message") continue;
    const newChannelId = channelMap.get(record.channelId);
    if (newChannelId !== undefined) {
      if (!topicsWriten[newChannelId]) {
        topicsWriten[newChannelId] = true;
        await writer.addMessage({ ...record, channelId: newChannelId });
      } else {
        topicsWriten[newChannelId] = false;
      }
    }
  }

  await writer.end();
  await outputStream.close();
  await inputStream.close();

  console.log(`Copying ${outputFile} complete.`);
}
