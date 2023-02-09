from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
import argparse
from utils import insert_partition


PYFILES_DIR = './spark_pyfiles/music_chart/'

def soundcloud_tracks(df: DataFrame):
    return df.drop(
        'genre', '_id', 'kind', 
        'last_updated', 'query_urn', 'data_time'
    ).withColumn('collection', f.explode('collection')) \
        .withColumn('track_id', f.col('collection.track.id')) \
        .withColumn('name', f.col('collection.track.title')) \
        .withColumn('duration', f.col('collection.track.duration')) \
        .withColumn('source', f.lit('soundcloud')) \
        .withColumn('release_date', f.to_timestamp(
            f.when(f.col('collection.track.release_date').isNotNull(), f.col('collection.track.release_date')) \
            .otherwise(f.col('collection.track.created_at')))) \
        .withColumn('popularity', f.col('collection.track.likes_count')) \
        .withColumn('artist_id', f.col('collection.track.user_id')) \
        .dropDuplicates(['track_id', 'artist_id']) \
        .drop('collection')


def soundcloud_genres(df: DataFrame):
    df = df.select(f.explode('collection').alias('collection'))
    return df.select(
        f.col('collection.track.id').alias('track_id'), 
        f.col('collection.track.genre').alias('genre')
    ).drop('collection')


def soundcloud_artists(df: DataFrame):
    return df.select(
        f.col('full_name').alias('name'), 
            f.col('id').alias('artist_id'), 
        f.col('followers_count').alias('total_followers'),
        f.lit('soundcloud').alias('source')
    ).dropDuplicates(subset=['artist_id'])


def main(args):
    spark = SparkSession.Builder().\
        appName('music_chart.transform.soundcloud').getOrCreate()

    brMySQL_conf = spark.sparkContext.broadcast({
        'host': 'mysqldb',
        'user': args.mysql_login,
        'password': args.mysql_password
    })
    # Define mongo reader
    reader = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', args.mongo_uri) \
        .option('spark.mongodb.aggregate.pipeline', {"$match": {"data_time": f'"{args.runtime}"'}})

    # Get track
    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'soundcloud_top_tracks') \
        .load()
    
    tracks_df = soundcloud_tracks(df)
    tracks_df = tracks_df.fillna('No Artist', subset='artist_id')
    genres_df = soundcloud_genres(df)
    

    # Artists
    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'soundcloud_artists') \
        .load()

    artists_df = soundcloud_artists(df)
    artists_df = artists_df.fillna('No Artist', subset='artist_id')

    tracks_df.printSchema()
    genres_df.printSchema()
    artists_df.printSchema()
    # Write
    br_artists_cols = spark.sparkContext.broadcast(artists_df.columns)
    artists_df.rdd.coalesce(5).\
        foreachPartition(lambda partition: insert_partition(
            partition=partition, 
            mysql_conf=brMySQL_conf.value, 
            dbname='music_chart',
            dbtable='artists', 
            cols=br_artists_cols.value
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
            cols=br_tracks_cols.value
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
            cols=br_genres_cols.value
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