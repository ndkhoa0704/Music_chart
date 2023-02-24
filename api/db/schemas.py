from pydantic import BaseModel
from datetime import datetime
from typing import Union

class GeneralModel(BaseModel):
    requested_at = datetime.now().isoformat()


class Track(BaseModel):
    source: str
    duration: int | None
    popularity: int | None
    track_id: str
    artist_id: str | list[str]
    name: str
    release_date: datetime
    chart_date: datetime


class Artist(BaseModel):
    source: str
    artist_id: str | None
    name: str | None
    total_followers: int | None

class Genres(GeneralModel):
    data: list[str]

class ArtistGenres(Genres):
    artist: str

class TrackGenres(Genres):
    track_id: str

class Genre(BaseModel):
    track_id: str | None
    genre: str | None
    artist_id: str | None


class ResponseModel(GeneralModel):
    data: Union[
        list[Track],
        list[Artist],
        list[Genre]
    ]


class User(BaseModel):
    # id: int
    username: str
    password: str
    email: str | None
    # class Config:
        # orm_mode = True


class Token(BaseModel):
    access_token: str
    token_type: str