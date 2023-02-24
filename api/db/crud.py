from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.expression import Select, func
from . import models
from . import schemas
from datetime import datetime
    

def get_tracks(db: Session, source, track_id, release_date, chart_time, artist_id, limit):

    sql_filter = list(filter(lambda x: x is not None, [
        models.Tracks.source==source if source else None, 
        models.Tracks.track_id==track_id if track_id else None,
        models.Tracks.release_date==release_date if release_date else None,
        models.Tracks.artist_id==artist_id if artist_id else None
    ]))

    chart_time_local = None
    if chart_time is None:
        chart_time_local = db.scalar(
            Select(func.max(models.metadata.data_time)).\
                filter(func.datediff(models.metadata.data_time, datetime.now().isoformat()) == 0)
        )
        sql_filter.append(models.metadata.data_time==chart_time_local)
    else:
        sql_filter.append(models.metadata.data_time==func.RoundDateTime())


    result_query = db.query(
            models.Tracks.source,
            models.Tracks.duration, 
            models.Tracks.popularity, 
            models.Tracks.track_id, 
            func.group_concat(models.Tracks.artist_id).label('artists'),
            models.Tracks.name, 
            models.Tracks.release_date,
            models.metadata.data_time
        ).join(
            models.metadata, 
            (models.metadata.record_id == models.Tracks.id) &
            (models.metadata.record_type == 'tracks') &
            (models.metadata.data_time == chart_time_local)
        ).filter(*sql_filter).group_by(            
            models.Tracks.source,
            models.Tracks.duration, 
            models.Tracks.popularity, 
            models.Tracks.track_id, 
            models.Tracks.name, 
            models.Tracks.release_date,
            models.metadata.data_time
        )
    
    result = {
        'data': (list(map(lambda row: {
        'source': row[0],
        'duration': row[1],
        'popularity': row[2],
        'track_id': row[3],
        'artist_id': row[4].split(','),
        'name': row[5],
        'release_date': row[6],
        'chart_date': row[7]
        }, result_query.yield_per(1))))
    }
    return result



def get_artists(db: Session, track_id, artist_id, source,chart_date, limit):
    sql_filter = list(filter(lambda x: x is not None, [
        models.Artists.source==source if source else None, 
        models.Artists.artist_id==artist_id if artist_id else None,
    ]))

    chart_time_local = db.scalar(
        Select(func.max(models.metadata.data_time)).\
            filter(func.datediff(models.metadata.data_time, chart_date) == 0)
    )

    sql_filter.append(models.metadata.data_time == chart_time_local)
    query = db.query(models.Artists, models.metadata.data_time).\
            join(
                models.metadata, 
                (models.metadata.record_id == models.Artists.id) &
                (models.metadata.record_type == 'artists')
            ).\
            filter(*sql_filter).limit(limit=limit)
    
    if track_id:
        sql_filter.append(models.Tracks.id == track_id)
        query = db.query(
                models.Artists, 
                models.metadata.data_time
            ).\
            join(models.metadata, models.metadata.record_id == models.Artists.id).\
            join(models.Tracks, models.Tracks.artist_id == models.Artists.artist_id).\
            filter(*sql_filter).limit(limit=limit)
    
    result = {'data' : list(map(lambda row: {
        'source': row[0].source,
        'artist_id': row[0].artist_id,
        'name': row[0].name,
        'total_followers': row[0].total_followers,
        'chart_date': row[1]
        }, query.yield_per(1)))
    }

    return result


def get_genres(
    db: Session, 
    track_id: str,
    artist_id: str,
    limit: int
):
    
    if track_id is None and artist_id is None:
        result = db.query(models.Genres.genre).distinct().all()
        print(query)

        return {'data': [row[0] for row in result]}
    
    sql_filter = list(map(lambda x: x is not None, [
        models.Tracks.track_id == track_id if track_id else None,
        models.Tracks.artist_id == artist_id if artist_id else None
    ]))

    query = db.query(models.Genres, models.Tracks).\
            join(models.Tracks, models.Tracks.track_id == models.Genres.track_id
        ).filter(*sql_filter).limit(limit=limit)
    
    if track_id:
        print(query)
        return {'data': list(map(lambda row: {
            'track_id': row[0].track_id,
            'genre': row[0].genre
            }, query.yield_per(1)))
        }
        
    
    if artist_id:
        print(query)
        return {'data': list(map(lambda row: {
            'artist_id': row[1].artist_id,
            'genre': row[0].genre
            }, query.yield_per(1)))
        }
    

def get_user(db: Session, username: str):
    return db.query(models.User).filter(models.User.username==username).first()


def create_user(db: Session, user: schemas.User):
    db_user = models.User(
        email=user.email, 
        password=user.password, 
        username=user.username
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user