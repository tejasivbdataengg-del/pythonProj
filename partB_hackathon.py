# from spark_hackathon import partA_hackathon
from pyspark.sql.functions import monotonically_increasing_id, avg

from partA_hackathon import method1


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date, col, concat, current_date, current_timestamp, udf, year, month, count

    from allmethods import rmsplchar

    spark = SparkSession.builder.config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar").config(
        "spark.eventLog.enabled", "true").config("spark.eventLog.dir", "file:///tmp/spark-events").config(
        "hive.metastore.uris", "thrift://127.0.0.1:9083").enableHiveSupport().getOrCreate()
    spark.conf.set("hive.metastore.uris", "thrift://127.0.0.1:9083")
    # spark.sparkContext.setLogLevel("INFO")
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
    method1(spark)
    structinsinfo1schema = StructType([
        StructField("IssuerId", IntegerType(), True),
        StructField("IssuerId2", IntegerType(), True),
        StructField("BusinessDate", DateType(), True),
        StructField("StateCode", StringType(), True),
        StructField("SourceName", StringType(), True),
        StructField("NetworkName", StringType(), True),
        StructField("NetworkURL", StringType(), True),
        StructField("custnum", StringType(), True),
        StructField("MarketCoverage", StringType(), True),
        StructField("DentalOnlyPlan", StringType(), True)
    ])

    structinsinfo2schema = StructType([
        StructField("IssuerId", IntegerType(), True),
        StructField("IssuerId2", IntegerType(), True),
        StructField("BusinessDate", StringType(), True),
        StructField("StateCode", StringType(), True),
        StructField("SourceName", StringType(), True),
        StructField("NetworkName", StringType(), True),
        StructField("NetworkURL", StringType(), True),
        StructField("custnum", StringType(), True),
        StructField("MarketCoverage", StringType(), True),
        StructField("DentalOnlyPlan", StringType(), True)
    ])

    insdf1 = spark.read.option("header", True).option("escape", ",").option("mode", "dropmalformed").schema(
        structinsinfo1schema).csv("file:///home/hduser/insuranceinfo1.csv")
    # insdf1.printSchema()

    insdf2 = spark.read.option("header", True).option("escape", ",").option("mode", "dropmalformed").schema(
        structinsinfo2schema).csv("file:///home/hduser/insuranceinfo1.csv").withColumn("BusinessDate",
                                                                                       to_date(col("BusinessDate"),
                                                                                               format("yyyy-mm-dd")))
    # insdf2.printSchema()
    # insdf2.show()

    insdf1rename = insdf1.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")
    insdf2rename = insdf2.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm")
    print(insdf1rename.columns)
    print(insdf2rename.columns)

    issueridcomposite1 = insdf1.withColumn("Issueridcomposite",
                                           concat(col("IssuerId").cast("string"), col("IssuerId2").cast("string")))
    issueridcomposite1.show()

    issueridcomposite2 = insdf2.withColumn("Issueridcomposite",
                                           concat(col("IssuerId").cast("string"), col("IssuerId2").cast("string")))
    issueridcomposite2.show()

    insdf1 = insdf1.drop("DentalOnlyPlan")
    insdf2 = insdf2.drop("DentalOnlyPlan")
    print("check this", insdf1.show())

    insdf1sysdt = insdf1.withColumn("sysdt", current_date()).withColumn("systs", current_timestamp())

    collist = insdf1.columns
    print(insdf1sysdt.show())

    rmna = insdf1sysdt.dropna()
    print(rmna.show())
    print(rmna.count())
    # str1=(col("NetworkName").cast(StringType()))
    udf_rmsplchar = udf(rmsplchar)
    rmsplcol = insdf1sysdt.withColumn("NetworkName", udf_rmsplchar((col("NetworkName").cast("String"))))
    print("removed spl char", rmsplcol.show())
    rmsplcol.write.mode("overwrite").json("/user/hduser/hackathon/json")
    rmsplcol.write.mode("overwrite").csv("/user/hduser/hackathon/csv", sep="~", header=True)
    # rmsplcol.write.option('path','/user/hduser/hackathon/exttbl').mode("append").saveAsTable("newdb.hive_rmsplcol")
    custstates = spark.sparkContext.textFile("/user/hduser/custs_states.csv")
    custfilter = custstates.map(lambda x: x.split(",")).filter(lambda x: len(x) == 5)
    statesfilter = custstates.map(lambda x: x.split(",")).filter(lambda x: len(x) == 2)
    print(custfilter.collect())
    custstatesdf = spark.read.option("inferschema", "True").option("delimiter", ",").csv(
        "/user/hduser/custs_states.csv").toDF("custid", "fname", "lname", "age", "prof")
    statesfiltertempdf = custstatesdf.where("age is null and lname is null and prof is null")
    statesfilterdf = statesfiltertempdf.drop("age", "lname", "prof").select(col("custid").alias("statecode"),
                                                                            col("fname").alias("statedesc"))
    custfilterdf = custstatesdf.where("age is not null and lname is not null and prof is not null")
    print(statesfilterdf.show())
    custfilterdf.createOrReplaceTempView("custview")
    statesfilterdf.createOrReplaceTempView("statesview")
    insdf1sysdt.createOrReplaceTempView("insureview")
    spark.udf.register("remspecialcharudf", rmsplchar)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    insuredf1 = spark.sql(
        "select IssuerId, IssuerId2, BusinessDate, StateCode, SourceName, cast(NetworkName as int) as NetworkName,remspecialcharudf(NetworkName) as cleannetworkname, NetworkURL, custnum, MarketCoverage from insureview")
    insuredf1.createOrReplaceTempView("insuredf1view")
    insuredf2 = spark.sql(
        "select IssuerId, IssuerId2, BusinessDate, StateCode, SourceName, NetworkName,cleannetworkname, NetworkURL, custnum, MarketCoverage,current_date() as curdt, current_timestamp() as curts from insuredf1view")
    insuredf2.createOrReplaceTempView("insuredf2view")
    insuredf3 = spark.sql(
        "select IssuerId, IssuerId2, BusinessDate, StateCode, SourceName, NetworkName,cleannetworkname, NetworkURL, custnum, MarketCoverage,curdt,curts,year(BusinessDate) as yr, month(BusinessDate) as mth from insuredf2view")
    insuredf3.createOrReplaceTempView("insuredf3view")
    insuredf4 = spark.sql("""select IssuerId, IssuerId2, BusinessDate, StateCode, SourceName, 
    NetworkName,cleannetworkname, NetworkURL, custnum, MarketCoverage,curdt,curts,yr, mth , 
    case when NetworkURL like ('%https%') then "http secured" 
    when NetworkURL like ('%http%') then "http non secured" 
    else "no protocol" end as protocol from insuredf3view """)
    insuredf4.createOrReplaceTempView("insuredf4view")

    singledf = spark.sql("select distinct a.custnum, a.StateCode, a.IssuerId, a.IssuerId2, a.BusinessDate, "
                         "a.SourceName, a.NetworkName,a.cleannetworkname, a.NetworkURL, "
                         "a.MarketCoverage,a.curdt,a.curts,a.yr, a.mth, a.protocol, "
                         "b.statedesc,c.age,c.prof from insuredf4view a inner join "
                         "statesview b on a.StateCode=b.statecode inner join custview c on a.custnum=c.custid")

    singledf.write.mode("overwrite").parquet("/user/hduser/hackathon/parquet")
    singledf.write.mode("overwrite").csv("/user/hduser/hackathon/csv1")
    singledf.createOrReplaceTempView("singledfview")
    aggrdf = spark.sql(
        "select avg(age) as avgage, count(*) as count , statedesc, protocol, prof, row_number() over(partition by protocol order by count(*) desc ) as rno from singledfview group by statedesc, protocol, prof")
    aggrdf.write.mode("overwrite").saveAsTable("default.insureaggregated")
    aggrdf.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/defaultdb?user=root&password=Root123$", table="insureaggregated",
                      mode="overwrite", properties={"driver": 'com.mysql.jdbc.Driver'})
    aggrdf.show()


if __name__ == "__main__":
    main()
