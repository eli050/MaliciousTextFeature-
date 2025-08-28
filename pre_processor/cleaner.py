import cleantext

class TextCleaner:
    @staticmethod
    def clean_central(text):
        text = text.lower()
        return cleantext.clean(text,
                               lowercase=True,# Convert to lowercase
                               extra_spaces=True, # Remove extra white spaces
                               punct=True,# Remove all punctuations
                               stopwords=True,# Remove stop words
                               stemming=True,# Stem the words
                               reg= r'\b(?:https?://\S+|www\.\S+)',# Remove URLs
                               reg_replace= "",
                              )


