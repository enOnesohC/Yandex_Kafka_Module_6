from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from Parameters import Parameters
import json

def user_to_dict(user, ctx):
    """
    Сериализация объекта пользователя в dict.
    """
    return user

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def error_callback(err):
    print('Something went wrong: {}'.format(err))


class ProducerObj(Producer):
    def __init__(self, topic):
        self.topic = topic
        self.subject = self.topic + "-value"
        bootstrap_servers = "localhost:9094,localhost:9095,localhost:9096"
        self.schema_registry_url = "http://localhost:8081"

        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
        }
        super().__init__(producer_conf)

    def init_shema(self):
        self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
        try:
            latest = self.schema_registry_client.get_latest_version(self.subject)
            print(f"Schema is already registered for {self.subject}:\n{latest.schema.schema_str}")
        except Exception:
            schema_object = Schema(Parameters.USER_SCHEMA_STR, "JSON")
            schema_id = self.schema_registry_client.register_schema(self.subject, schema_object)
            print(f"Registered schema for {self.subject} with id: {schema_id}")

    def send_message(self, message):
        json_serializer = JSONSerializer(Parameters.USER_SCHEMA_STR, self.schema_registry_client, user_to_dict)
        user = message
        context = SerializationContext(self.topic, MessageField.VALUE)
        serialized_value = json_serializer(user, context)
        self.produce(
            self.topic,
            key=str(uuid4()),
            value=serialized_value,
            headers=[("myTestHeader", b"header values are binary")],
            callback=delivery_report,
        )
        self.flush()


class ProducerCloude(Producer):
    def __init__(self):
        params = {
            'bootstrap.servers': '<FQDN_хоста-брокера>:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': '<имя_производителя>',
            'sasl.password': '<пароль_производителя>',
            'error_cb': error_callback,
        }
        super().__init__(params)

    def send_message(self, topic, message):
        self.produce(topic, message)
        self.flush(10)
