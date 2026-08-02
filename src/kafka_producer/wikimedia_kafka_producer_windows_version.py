
# source code for wikimedia kafka producer
# https://github.com/sakthivelooty/wiki_data_analysis

from unittest.mock import call

from confluent_kafka import Producer
import time
import requests
from sseclient import SSEClient
from datetime import datetime
from pathlib import Path

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}


  # Anchors the path to the script's directory
  script_dir = Path(__file__).parent
  file_path = script_dir / 'client.properties'

  with open(file_path, 'r') as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


class WikimediaKafkaProducer:
    def __init__(self, kafka_config, topic_name, wikimedia_url, header):
        self.topic_name = topic_name
        self.wikimedia_url = wikimedia_url
        self.header = header
        self.producer = Producer(kafka_config)

    def on_message(self, event_data):
        # send data to kafka
        print(f"Sending ==> :{event_data[:100]}")
        self.producer.produce(
            self.topic_name,
            key=datetime.now().isoformat(),
            value=event_data
        )
        self.producer.flush()

    def start_streaming(self):
        print("Starting WIKIMEDIA Eventstream to kafka!!")
        start_time = time.time()
        end_time = start_time + (10 * 60)

        try:
            # Using standard requests stream with explicit timeouts
            response = requests.get(
                self.wikimedia_url,
                stream=True,
                headers=self.header,
                timeout=(10, 30)
            )
            response.raise_for_status()

            # Native line-by-line SSE parser to avoid Windows socket block
            current_event_data = []

            for line in response.iter_lines(decode_unicode=True):
                if time.time() > end_time:
                    print("Time limit of 10 minutes reached.")
                    break

                if line:
                    if line.startswith("data:"):
                        current_event_data.append(line[5:].strip())
                else:
                    # Empty line indicates end of an SSE message block
                    if current_event_data:
                        event_data = "\n".join(current_event_data)
                        print(f"Received ==> :{event_data[:100]}")
                        self.on_message(event_data)
                        current_event_data = []

        except Exception as e:
            print(f"Error in Stream reading: {e}")

        finally:
            self.producer.close()
            print("Kafka producer closed.")





if __name__ == "__main__":
  
  TOPIC_NAME = "wikimedia_analysis_2"
  WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
  WIKIMEDIA_HEADER = {
    'Accept': 'text/event-stream',
    'User-Agent': "WikiSreamDemo/1.0 (andy.osei.ao77@gmail.com)"
  }

  kafka_conf = read_config() 

  wikimedia_producer = WikimediaKafkaProducer(
    kafka_conf, TOPIC_NAME, WIKIMEDIA_URL, WIKIMEDIA_HEADER
  )

  wikimedia_producer.start_streaming()

