const { Kafka } = require("kafkajs");
require("dotenv").config();

const IP = process.env.IP;

createTopic();

async function createTopic() {
  // Admin stuff..
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_sore_client",
      brokers: [IP],
    });

    const admin = kafka.admin();
    console.log("kafka broker'a bağlanılıyor..");
    await admin.connect();
    console.log("kafka broker'a bağlanıldı.. Topic üretilecek");
    await admin.createTopics({
      topics: [
        {
          topic: "LogStoreTopic",
          numPartitions: 2,
        }
      ],
    });

    console.log("topic olusturuldu..");
    await admin.disconnect();
  } catch (error) {
    console.log("hata" ,error);
  } finally {
    process.exit();
  }
}
