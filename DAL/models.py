from pydantic import BaseModel


class TweetInDB(BaseModel):
    TweetID: int
    text: str
    Antisemitic: int
    CreateDate: str
    cleaned_text: str
    sentiment : str
    weapons_detected: list|None
    relevant_timestamp: str|None

