from cleaner import TextCleaner
from kafka_objects.consumer import Consumer
from kafka_objects.producer import Producer

ANTISEMITIC =  "raw_tweets_antisemitic"
NOT_ANTISEMITIC = "raw_tweets_not_antisemitic"

ANTISEMITIC_CLEANED = "preprocessed_tweets_antisemitic"
NOT_ANTISEMITIC_CLEANED = "preprocessed_tweets_not_antisemitic"

class PrP_controler:
    def __init__(self):
        self.text_cleaner = TextCleaner()
        self.consumer = Consumer([ANTISEMITIC,NOT_ANTISEMITIC]).get_consumer()
        self.producer = Producer()
    def process(self):
        try:
            for message in self.consumer:
                cleaned_text = self.text_cleaner.clean_central(message.value['text'])
                topic = message.topic
                message.value['cleaned_text'] = cleaned_text
                if topic == ANTISEMITIC:
                    self.producer.publish_message(ANTISEMITIC_CLEANED,message.value)
                else:
                    self.producer.publish_message(NOT_ANTISEMITIC_CLEANED,message.value)
        except Exception as e:
            print("Error in process " + str(e))
