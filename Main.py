from Producer import ProducerObj
from Consumer import ConsumerObj
from Parameters import Messages


producer = ProducerObj("user")

producer.init_shema()

producer.send_message(Messages.message1)
producer.send_message(Messages.message2)
producer.send_message(Messages.message3)
producer.send_message(Messages.message4)
producer.send_message(Messages.message5)
producer.send_message(Messages.message6)

consumer = ConsumerObj("user")

consumer.init_shema()

consumer.set_subscribe()
