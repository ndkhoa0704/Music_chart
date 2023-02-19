from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from db.database import SessionLocal
from sqlalchemy.orm import Session
from db import schemas
from datetime import datetime
from db import crud
from utils import dt_isoformat_regex


oath2_schema = OAuth2PasswordBearer(tokenUrl='token')

app = FastAPI()

@app.get('/')
def index():
    return {'greet': 'api'}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get('/tracks', response_model=schemas.ResponseModel)
def get_tracks(
    db: Session=Depends(get_db), 
    release_date: str | None = Query(
        default=None, 
        regex=dt_isoformat_regex
    ),
    source: str | None = None,
    track_id: str | None = None,
    artist_id: str | None = None,
    chart_date: str | None = Query(
        default=datetime.now().isoformat(), 
        regex=dt_isoformat_regex
    )   
):
    return crud.get_tracks(
        db, track_id=track_id, 
        release_date=release_date, 
        source=source,
        chart_date=chart_date,
        artist_id=artist_id
    )       


@app.get('/artists', response_model=schemas.ResponseModel)
def get_artists(
    db: Session=Depends(get_db),
    track_id: str | None = Query(default=None, max_length=32),
    artist_id: str | None = Query(default=None, max_length=32),
    source: str | None = None,
    chart_date: str | None = Query(
        default=datetime.now().isoformat(), 
        regex=dt_isoformat_regex
    )
):
    return crud.get_artists(
        db, track_id=track_id, chart_date=chart_date,
        artist_id=artist_id, source=source
    )

@app.get('/genre', response_model=schemas.ResponseModel)
def get_genre(
    db: Session=Depends(get_db),
    track_id: str | None = Query(default=None, max_length=32),
    artist_id: str | None = Query(default=None, max_length=32)
):
    # if track_id is None and artist_id is None:
        # return crud.get_all_genres(db)
    
    return crud.get_genres(db, track_id=track_id, artist_id=artist_id)