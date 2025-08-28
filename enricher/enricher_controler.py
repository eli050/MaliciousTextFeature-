from enrich_data import Enricher
from kafka_objects.consumer import Consumer
from kafka_objects.producer import Producer

ANTISEMITIC_CLEANED = "preprocessed_tweets_antisemitic"
NOT_ANTISEMITIC_CLEANED = "preprocessed_tweets_not_antisemitic"

ANTISEMITIC_ENRICHED = "enriched_preprocessed_tweets_antisemitic"
NOT_ANTISEMITIC_ENRICHED = "enriched_preprocessed_tweets_not_antisemitic"
class ERController:
    def __init__(self):
        self.consumer = Consumer([ANTISEMITIC_CLEANED,NOT_ANTISEMITIC_CLEANED]).get_consumer()
        self.producer = Producer()
        self.enricher = Enricher()
    def process(self):
        try:
            for message in self.consumer:
                print(message.value)
                enriched_data = self.enricher.process_data(message.value)
                topic = message.topic
                print(enriched_data)


                if topic == ANTISEMITIC_CLEANED:
                    self.producer.publish_message(ANTISEMITIC_ENRICHED,enriched_data)
                else:
                    self.producer.publish_message(NOT_ANTISEMITIC_ENRICHED,enriched_data)
        except Exception as e:
            print("Error in process " + str(e))
