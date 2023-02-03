from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse
import mysql.connector


def insert_row(row, cols: list, cursor, dbtable: str, replace: bool=False):
    'Insert row to database'
    command = 'INSERT'
    if replace:
        command = 'REPLACE'
    sql = f'''
    {command} INTO {dbtable}
    ({','.join(cols)})
    VALUES ({','.join(['%s']*len(cols))})
    '''
    cursor.execute(sql, row)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mysql_uri', dest='mysql_uri')
    parser.add_argument('--mongo_uri', dest='mongo_uri')
    parser.add_argument('--mysql_login', dest='mysql_login')
    parser.add_argument('--mysql_password', dest='mysql_password')
    parser.add_argument('--runtime', dest='runtime')
    args = parser.parse_args()

    spark = SparkSession.Builder().\
        appName('music_chart.transform').getOrCreate()
    
    pipeline = {
        "$match": {"data_time": f'"{args.runtime}"'}
    }


    reader = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', args.mongo_uri) \
        .option('spark.mongodb.aggregate.pipeline', pipeline)
    
    # Tracks

    soundcloud_tracks_df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'soundcloud_top_tracks') \
        .load()

    soundcloud_tracks_df = soundcloud_tracks_df.drop(
        'genre', '_id', 'kind', 
        'last_updated', 'query_urn'
    ).withColumn('collection', f.explode('collection')) \
        .withColumn('id', f.col('collection.track.id')) \
        .dropDuplicates(['id']) \
        .withColumn('name', f.col('collection.track.title')) \
        .withColumn('duration', f.col('collection.track.duration')) \
        .withColumn('source', f.lit('soundcloud')) \
        .withColumn('release_date', f.to_timestamp(
            f.when(f.col('collection.track.release_date').isNotNull(), f.col('collection.track.release_date')) \
            .otherwise(f.col('collection.track.created_at')))) \
        .withColumn('popularity', f.col('collection.track.likes_count')) \
        .withColumn('artist', f.col('collection.track.user_id')) \
        .withColumn('data_time', f.to_timestamp('data_time')) \
        .drop('collection')


    spotify_tracks_df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify_top_tracks') \
        .load()
    
    spotify_tracks_df = spotify_tracks_df.drop(
        'href', '_id', 'limit', 'total', 
        'next', 'offset', 'previous'
    ).withColumn('items', f.explode('items')) \
        .withColumn('id', f.col('items.track.id')) \
        .withColumn('name', f.col('items.track.name')) \
        .withColumn('duration', f.col('items.track.duration_ms')) \
        .withColumn('source', f.lit('spotify')) \
        .withColumn('release_date', f.to_timestamp(f.col('items.added_at'))) \
        .withColumn('popularity', f.col('items.track.popularity')) \
        .withColumn('artists', f.explode('items.track.artists')) \
        .withColumn('artist', f.col('artists.id')) \
        .withColumn('data_time', f.to_timestamp('data_time')) \
        .dropDuplicates(['id', 'artist']) \
        .drop('artists') \
        .drop('items')
    
    tracks_df = soundcloud_tracks_df.union(spotify_tracks_df)
    
    del soundcloud_tracks_df, spotify_tracks_df
    # Artists
    soundcloud_artists_df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'soundcloud_artists') \
        .load()

    soundcloud_artists_df = soundcloud_artists_df.select(
        f.col('full_name').alias('name'), 
        f.to_timestamp('data_time').alias('data_time'), 'id', 
        f.col('followers_count').alias('total_followers'),
        f.lit('soundcloud').alias('source')
    ).dropDuplicates(subset=['id'])
    
    spotify_artists_df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify_artists') \
        .load()

    spotify_artists_df = spotify_artists_df.select(
        'name', f.to_timestamp('data_time').alias('data_time'), 'id',
        f.col('followers.total').alias('total_followers'),
        f.lit('spotify').alias('source')
    ).dropDuplicates(subset=['id'])
    
    artists_df = soundcloud_artists_df.union(spotify_artists_df)

    del spotify_artists_df, soundcloud_artists_df
    # Write
    mysql_conf = {
        'host': 'mysqldb',
        'user': args.mysql_login,
        'password': args.mysql_password
    }

    brMySQL_conf = spark.sparkContext.broadcast(mysql_conf)
    br_tracks_cols = spark.sparkContext.broadcast(tracks_df.columns)
    br_artists_cols = spark.sparkContext.broadcast(artists_df.columns)

    def insert_tracks(partition):
        '''
        Insert records to mysql for each partition
        '''
        mysql_conf = brMySQL_conf.value
        mysql_conn = mysql.connector.connect(**mysql_conf, database='music_chart')
        cursor = mysql_conn.cursor()
        for row in partition:
            insert_row(row, br_tracks_cols.value, cursor, 'tracks', True)
        mysql_conn.commit()
        mysql_conn.close()

    def insert_artists(partition):
        '''
        Insert records to mysql for each partition
        '''
        mysql_conf = brMySQL_conf.value
        mysql_conn = mysql.connector.connect(**mysql_conf, database='music_chart')
        cursor = mysql_conn.cursor()
        for row in partition:
            insert_row(row, br_artists_cols.value, cursor, 'artists', True)
        mysql_conn.commit()
        mysql_conn.close()

    tracks_df.rdd.coalesce(5).\
        foreachPartition(insert_tracks)
    
    artists_df.rdd.coalesce(5).\
        foreachPartition(insert_artists)