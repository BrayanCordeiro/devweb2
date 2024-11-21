from confluent_kafka import Consumer, KafkaError
import json
import logging
import telegram
import asyncio

ID = '-4573981332'
KEY = '7695352564:AAEoXQzdTrMj-UHAbaCZSSkh9igo12X-PRU'

async def notificate(notification):
    b = telegram.Bot(token=KEY)
    await b.send_message(chat_id=ID, text=notification)

c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notification-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notification'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data.get('file')
            logging.warning(f"READING {filename}")
            asyncio.run(notificate(json.dumps(data)))
            logging.warning(f"ENDING {filename}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
