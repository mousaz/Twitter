const fs = require("fs");
const readline = require("readline");
const KafkaProducer = require("./KafkaProducer");

require("dotenv").config();

const logger = require("../jscommon/logger").createLogger(
  "FileSourceProducer:"
);

const FILE_LINES_BUFF_SIZE = process.env.FILE_LINES_BUFF_SIZE || 10;
const FILE_DELAY_BETWEEN_READS = process.env.FILE_DELAY_BETWEEN_READS || 500;

const [, , dataFilePath] = process.argv;

const kafka = new KafkaProducer(null, (err, report) => {
  if (err) {
    logger.warn("Error producing: ", err);
  } else {
    const { topic, key, value } = report;
    let k = key.toString().padEnd(10, " ");
    logger.log(`Produced event to topic ${topic}: key = ${k} value = ${value}`);
  }
});

kafka
  .start()
  .then(() => startConsumingFile())
  .catch((err) => {
    logger.error("Failed to start Kafka producer: ", err);
    process.exit(1);
  });

function startConsumingFile() {
  const file = readline.createInterface({
    input: fs.createReadStream(dataFilePath),
    output: process.stdout,
    terminal: false,
  });

  let totalReadItems = 0;
  let linesBuff = [];

  file.on("line", (line) => {
    if (linesBuff.length > FILE_LINES_BUFF_SIZE) {
      kafka.produce(linesBuff);
      linesBuff = [];
      file.pause();
      setTimeout(() => {
        file.resume();
      }, FILE_DELAY_BETWEEN_READS);
    }

    logger.log(`Read line: ${line}`);
    linesBuff.push(line);
    totalReadItems++;
  });

  file.on("close", () => {
    logger.log(
      `Read all content [${totalReadItems} lines] of the file > Exiting producer.`
    );
    process.exit();
  });
}
