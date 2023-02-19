from pydantic import BaseModel
from datetime import datetime


class GeneralModel(BaseModel):
    requested_at = datetime.now().isoformat()


class Track(BaseModel):
    source: str
    duration: int | None
    popularity: int | None
    track_id: str
    artist_id: str
    name: str
    release_date: datetime
    chart_date: datetime


class Artist(BaseModel):
    source: str
    artist_id: str | None
    name: str | None
    total_followers: int | None


class Genre(BaseModel):
    track_id: str | None
    genre: str | None
    artist_id: str | None


class ResponseModel(GeneralModel):
    data: list[Track] | \
        list[Artist] | \
        list[Genre]


class User