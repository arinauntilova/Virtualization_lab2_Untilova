from pyspark.sql import SparkSession

def init_spark():
    sql = SparkSession.builder\
        .master('spark://spark-master:7077')\
        .appName("House_prices")\
        .getOrCreate()
    sc = sql.sparkContext
    return sql, sc


def main():
    url = "jdbc:postgresql://postgres:5432/House_prices_db"
    properties = {
        "user": "arina_untilova",
        "password": "passw",
        "driver": "org.postgresql.Driver"
        }

    sql, sc = init_spark()

    df = spark.read.jdbc(url=url
                    , table="House_prices"
                    , properties=properties)
    

    df.createOrReplaceTempView("House_prices_tbl")

    spark.sql("""select location, 
                        bedrooms,
                        round(avg(price), 0) as avg_price
                from House_prices_tbl
                group by location, bedrooms
                order by avg_price desc""").show()

  
if __name__ == '__main__':
    main()


