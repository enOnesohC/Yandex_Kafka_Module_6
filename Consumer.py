from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from Parameters import Parameters

def user_from_dict(user, ctx):
   """
   Десериализация объекта пользователя из dict.
   """
   return user

def error_callback(err):
    print('Something went wrong: {}'.format(err))


class ConsumerObj(Consumer):
    def __init__(self, topic):
        self.schema_registry_url = "http://localhost:8081"
        self.topic = topic
        consumer_conf = {
           "bootstrap.servers": "localhost:9094,localhost:9095,localhost:9096",
           "group.id": "group",
           "auto.offset.reset": "earliest",
        }
        super().__init__(consumer_conf)
        self.subscribe([topic])

    def init_shema(self):
        self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
        latest = self.schema_registry_client.get_latest_version(self.topic + "-value")
        json_schema_str = latest.schema.schema_str
        self.json_deserializer = JSONDeserializer(json_schema_str, from_dict=user_from_dict)


    def set_subscribe(self):
        try:
            while True:
               msg = self.poll(1.0)
               if msg is None:
                   continue
               if msg.error():
                   print(f"Error: {msg.error()}")
                   break

               context = SerializationContext(msg.topic(), MessageField.VALUE)
               user = self.json_deserializer(msg.value(), context)
               print(f"Message on {msg.topic()}:\n{user}")
               if msg.headers():
                   print(f"Headers: {msg.headers()}")
        except KeyboardInterrupt as err:
            print(f"Consumer error: {err}")
        finally:
            self.close()
            print("Closing consumer")

class ConsumerClaude(Consumer):
    def __init__(self, topic):
        params = {
            'bootstrap.servers': Parameters.FQDN,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': '<имя_потребителя>',
            'sasl.password': '<пароль_потребителя>',
            'group.id': 'test-consumer1',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'debug': 'all',
        }
        super().__init__(params)
        self.subscribe([topic])

    def set_subscribe(self):
        while True:
            msg = self.poll(timeout=3.0)
            if msg:
                val = msg.value().decode()
                print(val)
