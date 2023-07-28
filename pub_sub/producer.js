const { Kafka } = require("kafkajs");

require("dotenv").config();

const IP = process.env.IP;

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: [IP],
    });

    const producer = kafka.producer();
    console.log("producer'a bağlanılıyor..");
    await producer.connect();
    console.log("producer'a bağlanıldı.. ");

    const msg_res = await producer.send({
      topic: "rav_video_topic",
      messages: [
        {
          value: "Yeni vido",
          partition: 0,
        },
      ],
    });
    console.log("Gönderim işlemi başarılı", JSON.stringify(msg_res));
    await producer.disconnect();
  } catch (error) {
    console.log("hata ", error);
  } finally {
    process.exit();
  }
}
