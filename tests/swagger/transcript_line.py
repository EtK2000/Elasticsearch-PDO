from typing import List


class TranscriptLine:
    def __init__(self, *, phrases: List[int], sentiment: float, tags: List[int], text: str, time: int,
                 topics: List[int]):
        self.phrases = phrases
        self.sentiment = sentiment
        self.tags = tags
        self.text = text
        self.time = time
        self.topics = topics
