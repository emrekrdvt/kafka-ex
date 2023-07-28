const { Kafka } = require("kafkajs");
require("dotenv").config();

const IP = process.env.IP;
const topic_name = process.argv[2] ||"Logs";
const partition = process.argv[3] || 0;
createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_ex_1",
      brokers: [IP],
    });

    const producer = kafka.producer();
    console.log("producer'a bağlanılıyor..");
    await producer.connect();
    console.log("producer'a bağlanıldı.. ");

    const msg_res = await producer.send({
      topic: topic_name,
      messages: [
        {
          value: "Bu bir test log Mesajıdır..",
          partition,
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
