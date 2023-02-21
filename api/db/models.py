from .database import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey


class metadata(Base):
    __tablename__ = 'data_time_metadata'
    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, index=True)
    data_time = Column(DateTime, index=True)
    record_type = Column(String)


class Tracks(Base):
    __tablename__ = 'tracks'
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String)
    duration = Column(Integer)
    popularity = Column(Integer)
    track_id = Column(String, index=True)
    artist_id = Column(String, ForeignKey('artists.id'), index=True)
    name = Column(String)
    release_date = Column(DateTime)


class Artists(Base):
    __tablename__ = 'artists'
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String)
    artist_id = Column(String, index=True)
    name = Column(String)
    total_followers = Column(Integer)


class Genres(Base):
    __tablename__ = 'track_genres'
    id = Column(Integer, primary_key=True, index=True)
    track_id = Column(String, index=True)
    genre = Column(String)


class User(Base):
    __tablename__ = 'user_data'
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String)
    password = Column(String)
    email = Column(String)
    