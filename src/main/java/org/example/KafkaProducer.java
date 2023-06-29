package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducer {
    private static final String TOPIC = "THUB.EVENT-UPSERTED";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Configuração das propriedades do produtor
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Criação do produtor Kafka

        try (Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props)) {
            // String a ser inserida como mensagem no tópico
            String jsonString = "{\"id\": 1, \"name\": \"Exemplo\"}";

            String jsonCerto = "{\"env\":\"test\",\"producer\":\"tardis-to-etl\",\"payload\":\"{\\\"event_upserted\\\":{\\\"customer_id\\\":\\\"45951272-2c54-4bbe-9346-a951c1751f39\\\",\\\"event_id\\\":\\\"1099f2e5-a61e-46df-8ef6-c101b04eb969\\\",\\\"domain\\\":\\\"testing\\\",\\\"event_type\\\":\\\"test\\\",\\\"last_updated_at\\\":\\\"2022-01-19T20:24:03.887Z\\\",\\\"links\\\":[{\\\"link_name\\\":\\\"test\\\",\\\"event_id\\\":\\\"a73adb12-8309-453c-be46-bf462d2da10e\\\"}]},\\\"meta\\\":{\\\"cid\\\":\\\"DEFAULT.HRN2E.WNIOD\\\",\\\"produced_at\\\":\\\"2023-04-13T03:29:25.595Z\\\",\\\"original_ip\\\":\\\"127.0.0.1\\\",\\\"deadletter_id\\\":\\\"4c029f40-4d12-4671-8174-19e5d5464edd\\\"}}\",\"signature\":\"AC8WYQwJPWbADYuXRU9Wutp13-nGhws0a37RuP6WfqCv_lguyJ-MReWfRZuvD4huzRephMiMV-CMfJmJvgAoJgSYUNHkabUu56DyR-98426dd_zsWYRdb0v30d3T1GLCAHvJ1jvirpD8gBZOOUXzdTTlgZa322cfmEywOBzlw78VpcMyMtj5QgwBkOmlZZj9rKC4QRIh4sS5c-mjyKZUImN5L-TTATtLEcDkfMsS0RLlcc0lh55PFLpI7bwgobR3poRQQ2x1gqYYnb8P56_0fwH8vJ1ecWve6bk1-rzsHjQgXFNOz5rzyk0-uymP5VCGinyDJHf-RtEIeapgz8OXlFWeaDVp-6EIAyt6n-dGhr8sLBd-t6ikJKLmRfvKqe3pw7xJgPih-dT-h10FqAlSaQq2PmRCzpua2a6nNY0mmm8LlN3V5H_kLY-M5PsPrFkVIXaHTGnyPxebhPvUllbfF2z85cof3xcoXWNNtE5RYzUkDbfbymtA0cXm28eFg-RHjbi4kD5A5d84HdOn024mkTVQNxJJj9SGwvvkJvg_VQCI3drjvUxS5rqZHYPkwUAR6UZrVEJdsq-05sbtOlZsuFhDtYgKnG_3wh-JAY38OPQ8ib_W1ff7qp1eqr2BGO5021OZev_IKDMjM5ZjlVMaTAXSIirAX_S_fwp8VY8KQKm45IZhPg\"}";
            String json = "{\n" +
                    "  \"env\": \"test\",\n" +
                    "  \"producer\": \"tardis-to-etl\",\n" +
                    "  \"payload\": {\n" +
                    "    \"event_upserted\": {\n" +
                    "      \"customer_id\": \"5f675031-2322-439e-ba97-61c66c89efb8\",\n" +
                    "      \"event_id\": \"6480acff-8001-36c7-9365-7c2614a8d2a6\",\n" +
                    "      \"event_type\": \"store_money_transaction\",\n" +
                    "      \"domain\": \"savings\",\n" +
                    "      \"payload\": {\n" +
                    "        \"store_money_transaction/id\": \"6480acff-8001-36c7-9365-7c2614a8d2a6\",\n" +
                    "        \"savings_account/id\": \"5f6752b9-1ce5-4e0f-a02f-6b4fdb0b4e38\",\n" +
                    "        \"store_money_transaction/source\": {\n" +
                    "          \"store_money_transaction.source/type\": \"transfer_in\",\n" +
                    "          \"store_money_transaction.source/id\": \"6480acff-3fb6-4c75-a06c-1de8b7198f28\"\n" +
                    "        },\n" +
                    "        \"store_money_transaction/amount\": 500,\n" +
                    "        \"store_money_transaction/post_date\": \"2023-06-07\"\n" +
                    "      },\n" +
                    "      \"last_updated_at\": \"2022-01-19T20:24:03.887Z\",\n" +
                    "      \"clock\": {},\n" +
                    "      \"version\": 1,\n" +
                    "      \"timestamp\": \"2022-01-19T20:24:03.887Z\",\n" +
                    "      \n" +
                    "      \"links\": [\n" +
                    "        {\n" +
                    "          \"link_name\": \"store_money_transaction/source\",\n" +
                    "          \"event_id\": \"6480acff-3fb6-4c75-a06c-1de8b7198f28\"\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    },\n" +
                    "    \"meta\": {\n" +
                    "      \"cid\": \"DEFAULT.HRN2E.WNIOD\",\n" +
                    "      \"produced_at\": \"2023-04-13T03:29:25.595Z\",\n" +
                    "      \"original_ip\": \"127.0.0.1\",\n" +
                    "      \"deadletter_id\": \"4c029f40-4d12-4671-8174-19e5d5464edd\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"signature\": \"AC8WYQwJPWbADYuXRU9Wutp13-nGhws0a37RuP6WfqCv_lguyJ-MReWfRZuvD4huzRephMiMV-CMfJmJvgAoJgSYUNHkabUu56DyR-98426dd_zsWYRdb0v30d3T1GLCAHvJ1jvirpD8gBZOOUXzdTTlgZa322cfmEywOBzlw78VpcMyMtj5QgwBkOmlZZj9rKC4QRIh4sS5c-mjyKZUImN5L-TTATtLEcDkfMsS0RLlcc0lh55PFLpI7bwgobR3poRQQ2x1gqYYnb8P56_0fwH8vJ1ecWve6bk1-rzsHjQgXFNOz5rzyk0-uymP5VCGinyDJHf-RtEIeapgz8OXlFWeaDVp-6EIAyt6n-dGhr8sLBd-t6ikJKLmRfvKqe3pw7xJgPih-dT-h10FqAlSaQq2PmRCzpua2a6nNY0mmm8LlN3V5H_kLY-M5PsPrFkVIXaHTGnyPxebhPvUllbfF2z85cof3xcoXWNNtE5RYzUkDbfbymtA0cXm28eFg-RHjbi4kD5A5d84HdOn024mkTVQNxJJj9SGwvvkJvg_VQCI3drjvUxS5rqZHYPkwUAR6UZrVEJdsq-05sbtOlZsuFhDtYgKnG_3wh-JAY38OPQ8ib_W1ff7qp1eqr2BGO5021OZev_IKDMjM5ZjlVMaTAXSIirAX_S_fwp8VY8KQKm45IZhPg\"\n" +
                    "}";


            // Criação da mensagem em formato JSON
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, json);

            // Envio da mensagem para o tópico
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        System.out.println("Erro ao enviar a mensagem: " + e.getMessage());
                    } else {
                        System.out.println("Mensagem enviada com sucesso. Offset: " + metadata.offset());
                    }
                }
            });

            // Aguarda a confirmação do envio por 1 segundo
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Fecha o produtor Kafka
    }
}