const { Kafka } = require("kafkajs");
require("dotenv").config();

const IP = process.env.IP;

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: [IP],
    });

    const consumer = kafka.consumer({
      groupId: "g_1k_2k_encoder_consumer_group",
    });
    console.log("Consumer'a bağlanılıyor..");

    await consumer.connect();

    console.log("Bağlantı başarılı");

    await consumer.subscribe({
      topic: "rav_video_topic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (res) => {
        console.log(
          `Gelen mesaj ${res.message.value} : Partition => ${res.partition}`
        );
      },
    });
  } catch (error) {
    console.log("hata", error);
  }
}
