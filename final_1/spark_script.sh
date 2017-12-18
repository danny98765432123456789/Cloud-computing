/usr/lib/spark/bin/spark-submit --master "local[4]"  \
                    			--conf "spark.mongodb.input.uri=mongodb://140.112.41.157:27018/cloud-final.parkingInfo?readPreference=primaryPreferred" \
                    			--conf "spark.mongodb.input.partitioner=MongoShardedPartitioner" \
                    			--conf "spark.mongodb.output.uri=mongodb://140.112.41.157:27018/cloud-final.parkinglots" \
                    			--packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 \
                    			spark_final.py