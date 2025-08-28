import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
import datefinder
import os

# dwl vader_lexicon
nltk_dir = os.path.join(os.getcwd(),"nltk_data")
try:
    nltk.download('vader_lexicon', download_dir=nltk_dir)
except Exception as e:
    print(f"Error downloading vader_lexicon: {e}")


class Enricher:
    """
    enrich the data
    """

    def __init__(self, weapons_file_path='../data/weapons.txt'):
        self.sid = SentimentIntensityAnalyzer()
        self.weapons_list = self._load_weapons(weapons_file_path)

    def _load_weapons(self, file_path):
        """load the weapons list from a file"""
        try:
            with open(file_path, 'r') as f:
                # read the file line by line
                return [line.strip().lower() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"worning:wheapons file not found: {file_path}")
            return []


    def _get_sentiment(self, text):
        """calculate the sentiment of the text"""
        if not isinstance(text, str):
            return "neutral"

        score = self.sid.polarity_scores(text)['compound']

        if score >= 0.5:
            return "positive"
        elif score <= -0.5:
            return "negative"
        else:
            return "neutral"

    def _find_weapons(self, text):
        """find the first weapon in the text."""
        if not isinstance(text, str):
            return ""
        weapons_found = []

        for weapon in self.weapons_list:
            # use regex to check if the weapon is in the text
            if re.search(r'\b' + re.escape(weapon) + r'\b', text):
                weapons_found.append(weapon)
        return weapons_found

    def _find_latest_date(self, text):
        """find the latest date in the text"""
        if not isinstance(text, str):
            return None
        dates = list(datefinder.find_dates(text))
        if not dates:
            return None
        latest_date = max(d.date() for d in dates)
        return latest_date.isoformat()  # Returns 'YYYY-MM-DD'
    def process_data(self, data):
        """
        the main function of the class. process the data and return a DataFrame.
        """
        if not data.get('text') or data['text'] == '':
            return data

        text = data['text']

        # 1. finding the latest date in text
        latest_date = self._find_latest_date(text)
        data['relevant_timestamp'] = latest_date

        # 2. finding the sentiment
        if not data.get('cleaned_text') or data['cleaned_text'] == '':
            return data
        text = data['cleaned_text']
        sentiment = self._get_sentiment(text)
        data['sentiment'] = sentiment

        # 3. finding the weapons name
        weapons = self._find_weapons(text)
        data['weapons_detected'] = weapons
        return data
