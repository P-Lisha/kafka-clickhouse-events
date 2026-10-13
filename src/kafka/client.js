const { Kafka, logLevel } = require("kafkajs");
const config = require("../config/env");

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  logLevel: logLevel.NOTHING,
});

module.exports = kafka;
