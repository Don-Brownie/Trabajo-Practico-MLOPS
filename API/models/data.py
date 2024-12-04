from pydantic import BaseModel

class Recommendation(BaseModel):
    advertiser: str
    model: str
    recommendations: list

class Stats(BaseModel):
    total_advertisers: int
    advertisers_with_most_variations: str
    model_agreement: float

class History(BaseModel):
    advertiser: str
    history: list
