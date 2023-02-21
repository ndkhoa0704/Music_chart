from fastapi import FastAPI, Query, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from db.database import SessionLocal
from sqlalchemy.orm import Session
from db import schemas
from datetime import datetime
from db import crud
from utils import dt_isoformat_regex
from datetime import timedelta
from security import (
    verify_password,
    check_token,
    get_hashed_password, 
    create_access_token
)


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


def get_current_user(db: Session = Depends(get_db), payload: dict = Depends(check_token)):
    cred_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    username = payload.get('sub')
    if not username:
        raise cred_exception
    user = crud.get_user(db, username=username)
    if not user:
        raise cred_exception
    return user


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
    ),
    user: schemas.User = Depends(get_current_user)
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
    ),
    user: schemas.User = Depends(get_current_user)
):
    return crud.get_artists(
        db, track_id=track_id, chart_date=chart_date,
        artist_id=artist_id, source=source
    )

@app.get('/genre', response_model=schemas.ResponseModel)
def get_genre(
    db: Session=Depends(get_db),
    track_id: str | None = Query(default=None, max_length=32),
    artist_id: str | None = Query(default=None, max_length=32),
    user: schemas.User = Depends(get_current_user)
):
    # if track_id is None and artist_id is None:
        # return crud.get_all_genres(db)
    
    return crud.get_genres(db, track_id=track_id, artist_id=artist_id)


@app.post('/token', response_model=schemas.Token)
async def request_token(
    db: Session = Depends(get_db), 
    form_data: OAuth2PasswordRequestForm = Depends()):
    cred_except = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    user = crud.get_user(db, form_data.username)
    if not user:
        raise cred_except
    if not verify_password(form_data.password, user.password):
        raise cred_except
    
    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(
        data={"sub": user.username}, expire_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.put('/newuser', status_code=status.HTTP_201_CREATED)
async def create_user(*, db: Session = Depends(get_db), User: schemas.User):
    User.password = get_hashed_password(User.password)
    user = crud.create_user(db, User)
    if not user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return {"detail": "User created"}