const Kafka = require("@confluentinc/kafka-javascript");

const logger = require("../jscommon/logger").createLogger("KafkaProducer:");

class KafkaProducer {
  #config = null;
  #onDeliveryReport = null;
  #producer = null;

  constructor(config, onDeliveryReport) {
    this.#config = config ?? {
      "bootstrap.servers": process.env.BOOTSTRAP_SERVERS,
      // Needed for delivery callback to be invoked
      dr_msg_cb: true,

      topic: process.env.KAFKA_TOPIC,
    };

    this.#onDeliveryReport = onDeliveryReport;
  }

  async start() {
    if (this.#producer) {
      throw new Error("Producer already started.");
    }

    this.#producer = await createProducer(this.#config, this.#onDeliveryReport);
  }

  async stop(flush = true) {
    if (this.#producer) {
      throw new Error("Producer not started.");
    }

    if (flush) {
      this.#producer.flush(lines.length, () => this.#producer.disconnect());
    }
  }

  async produce(lines, flush = true) {
    if (!this.#producer) {
      throw new Error("Producer not started.");
    }

    for (const line of lines) {
      const lineParsed = JSON.parse(line);
      const key = lineParsed.id;
      this.#producer.produce(this.#config.topic, -1, Buffer.from(line), key);
    }

    if (flush) {
      this.#producer.flush(lines.length);
    }
  }
}

function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(config);

  return new Promise((resolve, reject) => {
    producer
      .on("ready", () => resolve(producer))
      .on("delivery-report", onDeliveryReport)
      .on("event.error", (err) => {
        logger.warn("event.error", err);
        reject(err);
      });

    producer.connect();
  });
}

module.exports = KafkaProducer;
