# %%
import string
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='alice'
consumer.subscribe([topic])

# %%
# Text cleaning, same steps as in the word count lab
STOP_WORDS = {
    'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
    'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
    'being', 'below', 'between', 'both', 'but', 'by', 'can', 'could', 'did',
    'do', 'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from',
    'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers',
    'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is',
    'it', 'its', 'itself', 'just', 'me', 'might', 'more', 'most', 'must',
    'my', 'myself', 'no', 'nor', 'not', 'now', 'of', 'off', 'on', 'once',
    'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
    'same', 'shall', 'she', 'should', 'so', 'some', 'such', 'than', 'that',
    'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there',
    'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under',
    'until', 'up', 'upon', 'very', 'was', 'we', 'were', 'what', 'when',
    'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
    'would', 'you', 'your', 'yours', 'yourself', 'yourselves',
    's', 't', 'd', 'll', 'm', 'o', 're', 've', 'y',
}

PUNCTUATION = string.punctuation + '“”‘’«»—–…﻿'
PUNCT_TABLE = str.maketrans({char: ' ' for char in PUNCTUATION})


def clean(line):
    """Lower case, remove punctuation, split in words, remove stop words."""
    words = line.lower().translate(PUNCT_TABLE).split()
    return [word for word in words if word not in STOP_WORDS]


# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

output_file = 'alice_cleaned.txt'

with open(output_file, 'a', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        # 1. Handle "No Message" (Timeout)
        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        # 2. Handle Errors
        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        # 3. Handle Success
        # Reset counters when we actually get data
        empty_polls = 0
        error_count = 0

        # Clean the line and write the words to the output file
        words = clean(msg.value().decode('utf-8'))
        if words:
            out.write(' '.join(words) + '\n')
            print(f"Rcvd message: {' '.join(words)}")

# Clean up
consumer.close()
