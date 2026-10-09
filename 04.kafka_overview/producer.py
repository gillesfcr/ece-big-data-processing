# %%
import socket
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='alice'
# book downloaded from Project Gutenberg:
# https://www.gutenberg.org/ebooks/11.txt.utf-8
book='data/alice_in_wonderland.txt'

# %% Read the book line by line and send each line to the topic
lines_sent = 0
with open(book, encoding='utf-8') as f:
  for line in f:
    producer.produce(
      topic=topic,
      value=line.rstrip('\n')
    )
    lines_sent += 1

print(f'{lines_sent} lines sent to the topic {topic}')
producer.flush()
producer.close()
