const { Kafka } = require("kafkajs");
require("dotenv").config();

const IP = process.env.IP;

const topic_name = process.argv[2] ||"Logs2";

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_ex_1",
      brokers: [IP],
    });

    const consumer = kafka.consumer({
      groupId: "ex_1_cg_1",
    });
    console.log("Consumer'a bağlanılıyor..");

    await consumer.connect();

    console.log("Bağlantı başarılı");

    await consumer.subscribe({
      topic: topic_name,
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
