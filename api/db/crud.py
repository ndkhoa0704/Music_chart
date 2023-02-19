from sqlalchemy.orm import Session
from sqlalchemy import func, Select, distinct
from . import models
    

def get_tracks(db: Session, source, track_id, release_date, chart_date, artist_id):
    sql_filter = list(filter(lambda x: x is not None, [
        models.Tracks.source==source if source else None, 
        models.Tracks.track_id==track_id if track_id else None,
        models.Tracks.release_date==release_date if release_date else None,
        models.Tracks.artist_id==artist_id if artist_id else None
    ]))
    max_chart_time = db.scalar(
        Select(func.max(models.metadata.data_time)).\
            filter(func.datediff(models.metadata.data_time, chart_date) == 0)
    )
    sql_filter.append(models.metadata.data_time == max_chart_time)
    query = db.query(models.Tracks, models.metadata.data_time).\
            join(
                models.metadata, 
                (models.metadata.record_id == models.Tracks.id) &
                (models.metadata.record_type == 'tracks')
        ).\
            filter(*sql_filter)
    
    result = {
        'data': list(map(lambda row: {
        'source': row[0].source,
        'duration': row[0].duration,
        'popularity': row[0].popularity,
        'track_id': row[0].track_id,
        'artist_id': row[0].artist_id,
        'name': row[0].name,
        'release_date': row[0].release_date,
        'chart_date': row[1]
        }, query.yield_per(1)))
    }

    return result


def get_artists(db: Session, track_id, artist_id, source,chart_date):
    sql_filter = list(filter(lambda x: x is not None, [
        models.Artists.source==source if source else None, 
        models.Artists.artist_id==artist_id if artist_id else None,
    ]))

    max_chart_time = db.scalar(
        Select(func.max(models.metadata.data_time)).\
            filter(func.datediff(models.metadata.data_time, chart_date) == 0)
    )
    sql_filter.append(models.metadata.data_time == max_chart_time)
    query = db.query(models.Artists, models.metadata.data_time).\
            join(
                models.metadata, 
                (models.metadata.record_id == models.Artists.id) &
                (models.metadata.record_type == 'artists')
            ).\
            filter(*sql_filter)
    
    if track_id:
        sql_filter.append(models.Tracks.id == track_id)
        query = db.query(
                models.Artists, 
                models.metadata.data_time
            ).\
            join(models.metadata, models.metadata.record_id == models.Artists.id).\
            join(models.Tracks, models.Tracks.artist_id == models.Artists.artist_id).\
            filter(*sql_filter)
    
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
):
    sql_filter = list(map(lambda x: x is not None, [
        models.Tracks.track_id == track_id if track_id else None,
        models.Tracks.artist_id == artist_id if artist_id else None
    ]))

    query = db.query(models.Genres, models.Tracks).\
    join(models.Tracks, models.Tracks.track_id == models.Genres.track_id
    ).filter(*sql_filter)

    return {'data': list(map(lambda row: {
        'track_id': row[0].track_id,
        'genre': row[0].genre,
        'artist_id': row[1].artist_id
        }, query.yield_per(1)))
    }