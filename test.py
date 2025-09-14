from confluent_kafka import Consumer, TopicPartition
import json

conf = {
    'bootstrap.servers': 'rc1a-qhklgd4in46f7q58.mdb.yandexcloud.net:9091',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'producer_consumer',
    'sasl.password': '12345678',
    'ssl.ca.location': r'C:\Users\lrr61\.kafka\YandexInternalRootCA.crt',
    'group.id': 'test_consumer_all',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = "order-service_orders"

# Подписка на топик и принудительное чтение с самого начала
def reset_offsets(consumer, partitions):
    partitions = [TopicPartition(p.topic, p.partition, offset=0) for p in partitions]
    consumer.assign(partitions)

consumer.subscribe([topic], on_assign=reset_offsets)
print(f"Подписались на топик: {topic}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Ошибка:", msg.error())
            continue
        print(f"Offset: {msg.offset()}, Key: {msg.key()}, Value: {json.loads(msg.value())}")
except KeyboardInterrupt:
    print("Завершение работы consumer")
finally:
    consumer.close()
