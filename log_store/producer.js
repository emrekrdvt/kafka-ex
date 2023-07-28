const { Kafka } = require("kafkajs");
const log_Data = require("./system_logs.json");
require("dotenv").config();

const IP = process.env.IP;

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_sore_client",
      brokers: [IP],
    });

    const producer = kafka.producer();
    console.log("producer'a bağlanılıyor..");
    await producer.connect();
    console.log("producer'a bağlanıldı.. ");
    let messages = log_Data.map((i) => {
      return {
        value: JSON.stringify(i),
        partition: i.type == "system" ? 0 : 1,
      };
    });

    const msg_res = await producer.send({
      topic: "LogStoreTopic",
      messages: messages,
    });
    console.log("Gönderim işlemi başarılı", JSON.stringify(msg_res));
    await producer.disconnect();
  } catch (error) {
    console.log("hata ", error);
  } finally {
    process.exit();
  }
}
