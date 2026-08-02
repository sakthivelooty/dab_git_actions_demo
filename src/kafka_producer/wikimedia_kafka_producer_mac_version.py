# Source code for the Wikimedia Kafka Producer - 
#  https://github.com/sakthivelooty/wiki_data_analysis


from unittest.mock import call

from confluent_kafka import Producer
import time
import requests
from sseclient import SSEClient
from datetime import datetime


def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config



class WikimediaKafkaProducer: 
  
  def on_message(self, event_deta):
    # send data to kafka

    print(f"Sending ==> :{event_deta[:100]}")
    self.producer.produce(self.topic_name, key = datetime.now().isoformat(), value = event_deta)
    self.producer.flush()
  
  def start_streaming(self):
    print("Starting WIKIMEDIA Evensteram to kafka!!")

    # get response
    response  = requests.get(self.wikimedia_url, stream=True, headers=self.header)
    client = SSEClient(response.raw)

    start_time = time.time()
    end_time = start_time + (10 * 60)
    # process response with sse
    try:
      for event in client.events():
        if time.time() > end_time:
            print(f"Time limit of 10 minutes reached.")
            break
        
        if event.event == 'message':
          if event.data:
            # send
            self.on_message(event.data.replace("\\n", ""))

    except Exception as e:
      print(f"Error in Stream reading {e}")

    finally:
      self.producer.close()

  
  def __init__(self, kafka_config, topic_name, wikimedia_url, header):
    
    self.topic_name = topic_name
    self.wikimedia_url = wikimedia_url
    self.header = header

    self.producer = Producer(kafka_config)



if __name__ == "__main__":
  
  TOPIC_NAME = "wikimedia_analisys"
  WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

  WIKIMEDIA_HEADER = {
    'Accept': 'text/event-stream',
    'User-Agent': "WikiSreamDemo/1.0 (yourmailid@examole.com)"
  }

  kafka_conf = read_config() 

  wikimedia_producer = WikimediaKafkaProducer(
    kafka_conf, TOPIC_NAME, WIKIMEDIA_URL, WIKIMEDIA_HEADER
  )

  wikimedia_producer.start_streaming()



#   https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q57085|Q603858&languages=en&format=json

#   bunch/bulk api calls,
#   sleep


# cache = []
# counter = 0
# for all qid in df1.distinct():
#     if countre> 100 
#         sleep(1)

#     else
#     if qid not in cache:
#         do api call on a qid 
#             if status code == 200
#                 cache.appedn(qid)
