import fs from "fs/promises";
import { McapWriter, McapIndexedReader } from "@mcap/core";
import { FileHandleReadable, FileHandleWritable } from "@mcap/nodejs";
import { loadDecompressHandlers } from "@mcap/support";
import path from "path";
import zstd from "@lichtblick/wasm-zstd";

// Read the input files from the command line argument
const inputFiles = process.argv.slice(2);

if (inputFiles.length === 0) {
  console.error(
    "Usage: npm run split <input-file1> <input-file2> <input-file3>"
  );
  process.exit(1);
}

inputFiles.forEach((inputFile) => {
  splitMcapByTopic(inputFile).catch(console.error);
});

async function splitMcapByTopic(inputFile: string) {
  const inputStream = await fs.open(inputFile, "r");
  const reader = await McapIndexedReader.Initialize({
    readable: new FileHandleReadable(inputStream),
    decompressHandlers: await loadDecompressHandlers(),
  });

  const writers = new Map<
    string,
    { writer: McapWriter; outputStream: fs.FileHandle }
  >();

  const baseName = path.basename(inputFile, ".mcap");

  for await (const record of reader.readMessages()) {
    const { channelId } = record;
    const channel = reader.channelsById.get(channelId);
    if (!channel) {
      continue;
    }

    if (record.type !== "Message") {
      continue;
    }

    let newChannelId = 0;

    const topic = channel.topic;

    if (topic != "/sensorik/axis_main/image/compressed") {
      continue;
    }

    if (!writers.has(topic)) {
      const outputDir = path.join(baseName);
      await fs.mkdir(outputDir, { recursive: true }); // Ensure directory exists
      const outputFile = `${outputDir}/${topic
        .replace(/\//g, "_")
        .replace(/^_/, "")}.mcap`;
      const outputStream = await fs.open(outputFile, "w");
      const writer = new McapWriter({
        writable: new FileHandleWritable(outputStream),
        compressChunk: (data) => ({
          compression: "zstd",
          compressedData: zstd.compress(data),
        }),
      });
      const schema = reader.schemasById.get(channel.schemaId);
      const newSchemaId = await writer.registerSchema(schema);
      newChannelId = await writer.registerChannel({
        ...channel,
        schemaId: newSchemaId,
      });
      writers.set(topic, { writer, outputStream });
      await writer.start({ library: "test", profile: "split-by-topic" });
    }

    const { writer } = writers.get(topic);
    await writer.addMessage({
      ...record,
      channelId: newChannelId,
    });
    console.log(`[${baseName}] Adding message to ${topic}`);
  }

  await inputStream.close();

  for (const { writer, outputStream } of writers.values()) {
    await writer.end();
    await outputStream.close();
  }

  console.log(`Splitting ${baseName} complete.`);
}
