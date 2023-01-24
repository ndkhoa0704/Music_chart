from pyspark.sql import SparkSession
import argparse
from pyspark.sql.types import (
    StructType, StructField 
)
import os
import logging
from pyspark.sql import functions as F

def sc_transform(row):
    return


# def sp_transform(row):
#     return Row(
#         artist_name=json_path('track.artists.[*].name', row),
#         track_name=json_path('track.name') 
#     )
                                             


def main(args):
    # logging.info('args: {args}')
    spark = SparkSession.Builder().\
        appName('transform_music_chart').getOrCreate()
    
    # spark.sparkContext.addPyFile(CUR_DIR)
    
    reader = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', args.uri) \
                
    # CUR_DIR = os.path.abspath(os.path.dirname(__file__))


    # # Read soundcloud
    # df = reader.option('spark.mongodb.database', 'music_chart') \
    #     .option('spark.mongodb.collection', 'soundcloud') \
    #     .option('spark.mongodb.aggregation.pipeline', pipeline) \
    #     .load()
                
    # df.show()

    # df = df.select(
    #         explode(df['collection']).alias('tracks')
    #     ).filter((to_timestamp(df['data_time'])) == to_timestamp(lit(args.runtime)))
    
    # df.foreach(sc_json_parse)

    # Read spotify
    pipeline = [
        {
            "$match": {
                "data_time": f"{args.runtime}"
            }
        },
        {
            "$project": {
                "_id": 0,
                "items": 1
            }
        }
        ,
        {
            "$unwind": {
                "path": "$items"
            }
        }
    ]
    df = reader.option('spark.mongodb.database', 'music_chart') \
        .option('spark.mongodb.collection', 'spotify') \
        .option('spark.mongodb.aggregation.pipeline', pipeline) \
        .load()
    
    df.printSchema()
    df.show()


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--uri', dest='uri')
    parser.add_argument('--runtime', dest='runtime')
    main(parser.parse_args())