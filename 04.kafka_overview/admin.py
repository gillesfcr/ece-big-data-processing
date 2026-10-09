# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config =  {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
# new topic for the lab, it will receive the lines of the book
topic='alice'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)
# wait for the creation to finish before listing the topics
for name, future in futures.items():
  try:
    future.result()
    print(f'Topic {name} created')
  except Exception as e:
    print(f'Topic {name} not created: {e}')

# %%
x = admin_client.list_topics()
for  t in x.topics.keys():
  print(t)

# %%
#admin_client.delete_topics([topic])

# %%
