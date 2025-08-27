import cleantext

class TextCleaner:

    def clean_central(self, text):
        return cleantext.clean(text,
                               lowercase=True,# Convert to lowercase
                               extra_spaces=True, # Remove extra white spaces
                               punct=True,# Remove all punctuations
                               stopwords=True,# Remove stop words
                               stemming=True,# Stem the words
                              )


