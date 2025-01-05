const { MongoClient } = require("mongodb");
require("dotenv").config();

const uri = process.env.MONGODB_URI || "localhost:27017";
const logger = require("../jscommon/logger").createLogger("MongoDBClient:");

class MongoDBClient {
  #collection = null;
  #client = null;
  #database = null;

  constructor() {
    logger.log("Initializing client.");
    this.#client = new MongoClient(uri);
    this.#database = this.#client.db(process.env.MONGODB_DATABASE_NAME);
    this.#collection = this.#database.collection(
      process.env.MONGODB_COLLECTION_NAME
    );
  }

  async dispose() {
    if (this.#client) {
      await this.#client.close();
    }
  }

  async runQuery(query) {
    try {
      logger.log(`Running query: ${JSON.stringify(query)}`);
      return await this.#collection.find(query).toArray();
    } catch (err) {
      logger.error(`Failed to run query. Error: ${err}`);
      return [];
    }
  }
}

module.exports = MongoDBClient;
