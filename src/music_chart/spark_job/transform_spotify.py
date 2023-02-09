from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
import argparse
from utils import insert_partition


PYFILES_DIR = '/opt/spark/spark_pyfiles/music_chart/'

def spotify_tracks(df: DataFrame):
    return df.drop(
        'href', '_id', 'limit', 'total', 
        'next', 'offset', 'previous', 'data_time'
    ).withColumn('items', f.explode('items')) \
        .withColumn('track_id', f.col('items.track.id')) \
        .withColumn('name', f.col('items.track.name')) \
        .withColumn('duration', f.col('items.track.duration_ms')) \
        .withColumn('source', f.lit('spotify')) \
        .withColumn('release_date', f.to_timestamp(f.col('items.added_at'))) \
        .withColumn('popularity', f.col('items.track.popularity')) \
        .withColumn('artists', f.explode('items.track.artists')) \
        .withColumn('artist_id', f.col('artists.id')) \
        .dropDuplicates(['track_id', 'artist_id']) \
        .drop('artists') \
        .drop('items')


def spotify_artists(df: DataFrame):
    return df.select(
        'name', f.col('id').alias('artist_id'),
        f.col('followers.total').alias('total_followers'),
        f.lit('spotify').alias('source'),
    ).dropDuplicates(subset=['artist_id'])


def main(args):
    spark = SparkSession.Builder().\
        appName('music_chart.transform.spotify').getOrCreate()

    brMySQL_conf = spark.sparkContext.broadcast({
        'host': 'mysqldb',
        'user': args.mysql_login,
        'password': args.mysql_password
    })

    # Define mongo reader
    reader = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', args.mongo_uri) \
        .option('spark.mongodb.aggregate.pipeline', {"$match": {"data_time": f'"{args.runtime}"'}})
    

    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify_top_tracks') \
        .load()
    
    tracks_df = spotify_tracks(df)

    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify_artists') \
        .load()
    
    artists_df = spotify_artists(df)
    
    genres_df = tracks_df.join(df, [tracks_df['artist_id'] == df['id']])\
        .select(f.explode('genres').alias('genre'), tracks_df['track_id'])

    # Write
    br_artists_cols = spark.sparkContext.broadcast(artists_df.columns)
    artists_df.rdd.coalesce(5).\
        foreachPartition(lambda partition: insert_partition(
            partition=partition, 
            mysql_conf=brMySQL_conf.value, 
            dbname='music_chart',
            dbtable='artists', 
            cols=br_artists_cols.value, 
            replace=True
        )
    )
    br_artists_cols.unpersist()
    del artists_df
    br_tracks_cols = spark.sparkContext.broadcast(tracks_df.columns)
    tracks_df.rdd.coalesce(5).\
        foreachPartition(lambda partition: insert_partition(
            partition=partition, 
            mysql_conf=brMySQL_conf.value, 
            dbname='music_chart',
            dbtable='tracks', 
            cols=br_tracks_cols.value, 
            replace=True
        )
    )
    br_tracks_cols.unpersist()
    del tracks_df

    br_genres_cols = spark.sparkContext.broadcast(genres_df.columns)
    genres_df.rdd.coalesce(5).\
        foreachPartition(lambda partition: insert_partition(
            partition=partition, 
            mysql_conf=brMySQL_conf.value, 
            dbname='music_chart',
            dbtable='track_genres', 
            cols=br_genres_cols.value, 
            replace=True
        )
    )
    br_genres_cols.unpersist()
    del genres_df

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mysql_uri', dest='mysql_uri')
    parser.add_argument('--mongo_uri', dest='mongo_uri')
    parser.add_argument('--mysql_login', dest='mysql_login')
    parser.add_argument('--mysql_password', dest='mysql_password')
    parser.add_argument('--runtime', dest='runtime')
    args = parser.parse_args()
    main(args)
