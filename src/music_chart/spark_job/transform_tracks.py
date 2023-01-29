from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse
import logging


def main(args):
    spark = SparkSession.Builder().\
        appName('transform_music_chart').getOrCreate()
    
    reader = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', args.mongo_uri) \
    
    # Soundcloud
    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'soundcloud_top_tracks') \
        .load()
    
    df = df.drop(
        'genre', '_id', 'kind', 
        'last_updated', 'query_urn'
    )

    df1 = df.withColumn('collection', f.explode('collection')) \
        .withColumn('id', f.col('collection.track.id')) \
        .dropDuplicates(['id']) \
        .withColumn('name', f.col('collection.track.title')) \
        .withColumn('duration', f.col('collection.track.duration')) \
        .withColumn('source', f.lit('soundcloud')) \
        .withColumn('release_date', f.to_timestamp(
            f.when(f.col('collection.track.release_date').isNull(), None) \
            .otherwise(f.col('collection.track.created_at')))) \
        .withColumn('popularity', f.col('collection.track.likes_count')) \
        .drop('collection')


    # Spotify
    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify_top_tracks') \
        .load()
    
    df = df.drop(
        'href', '_id', 'limit', 'total', 
        'next', 'offset', 'previous'
    )

    df2 = df.withColumn('items', f.explode('items')) \
        .withColumn('id', f.col('items.track.id')) \
        .dropDuplicates(['id']) \
        .withColumn('name', f.col('items.track.name')) \
        .withColumn('duration', f.col('items.track.duration_ms')) \
        .withColumn('source', f.lit('spotify')) \
        .withColumn('release_date', f.to_timestamp(f.col('items.added_at'))) \
        .withColumn('popularity', f.col('items.track.popularity')) \
        .drop('items')

    df = df1.union(df2)

    del df1, df2
    logging.info('Writing data...')

    df.write.mode('overwrite').format('jdbc') \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://mysqldb:3306") \
    .option("dbtable", "music_chart.tracks") \
    .option("user", "root") \
    .option("password", "root").save()

    del df


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mysql_uri', dest='mysql_uri')
    parser.add_argument('--mongo_uri', dest='mongo_uri')
    parser.add_argument('--runtime', dest='runtime')
    main(parser.parse_args())