from jose import jwt, JWTError
from passlib.context import CryptContext
import os
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer


pwd_contexet= CryptContext(schemes=['bcrypt'], deprecated='auto')
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


SECRET_KEY = os.getenv('SECRET_API_KEY')
ALGORITHM = 'HS256'


def get_hashed_password(password: str) -> str:
    return pwd_contexet.hash(password)


def verify_password(plain_password, hashed_password):
    return pwd_contexet.verify(plain_password, hashed_password)


def create_access_token(data: dict, expire_delta: timedelta | None = None):
    data_cp = data.copy()
    if expire_delta:
        expire = datetime.utcnow() + expire_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    data_cp.update({'exp': expire})
    return jwt.encode(data_cp, SECRET_KEY, algorithm=ALGORITHM)


async def check_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        print(payload)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload
