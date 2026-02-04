from pyspark.sql import Row
from pyspark.sql import SparkSession
def method1(sparkess):
    # spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    insuredata=sparkess.sparkContext.textFile("hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo1.csv")
    print(insuredata.count())
    count1=insuredata.count()
    print(count1)
    header=insuredata.first()
    filteredinsuredata=insuredata.filter((lambda i:i!=header))
    print("first row after removing header",filteredinsuredata.first())
    print("before removing footer",filteredinsuredata.count())
    noheaderandfooter=filteredinsuredata.filter(lambda x:x!="footer count is 402")
    print("after removing footer",noheaderandfooter.count())
    mappedrdd=noheaderandfooter.map(lambda x:x.split(","))
# def func(y,j):
#     for i in y:
#         if(i==None or i==''):
#             j=1
#     if(j==0):
#      return True
    rmblankrow=mappedrdd.filter(lambda x:len(x)==10)
    rejectdata=mappedrdd.filter(lambda x:len(x)!=10).map(lambda x:(len(x),x))
    count2=rmblankrow.count()
    print(count2)
    print(count1-count2)
    print(rmblankrow.count())
    print(rejectdata.collect())

    insuredata2=sparkess.sparkContext.textFile("hdfs://127.0.0.1:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
    print(insuredata2.count())
    countdata21=insuredata2.count()
    print(countdata21)
    header2=insuredata2.first()
    print("first row before removing header",insuredata2.first())
    filteredinsuredata2=insuredata2.filter((lambda i:i!=header2))
    print("first row after removing header",filteredinsuredata2.first())
    print("before removing footer",filteredinsuredata2.count())
    noheaderandfooter2=filteredinsuredata2.filter(lambda x:x!="footer count is 3333")
    print("after removing footer",noheaderandfooter2.count())
    mappedrdd2=noheaderandfooter2.map(lambda x:x.split(","))
    def func(y):
        if(y[0]==None or y[0]==''):
         return False
        else:
            return True
    rmblankcol=mappedrdd2.filter(lambda x:len(x)==10).filter(lambda x:func(x))
    rmblankrow2=mappedrdd2.filter(lambda x:len(x)==10)
    rejectdata2=mappedrdd2.filter(lambda x:len(x)!=10).map(lambda x:(len(x),x))
    countdata22=rmblankrow2.count()
    print(countdata22)
    print(countdata21-countdata22)
    print(rmblankcol.count())
# header2rdd=insuredata2.filter(lambda x:x==header2).map(lambda x:x.split(","))
    insuredatamerged=rmblankcol.map(lambda p : Row(IssuerId=p[0],IssuerId2= p[1], BusinessDate=p[2],StateCode=p[3], SourceName=p[4], NetworkName=p[5], NetworkURL=p[6], custnum=int(p[7]), MarketCoverage=p[8],DentalOnlyPlan=p[9]))
    insuredatamerged.persist()
    if insuredatamerged.count() == rmblankcol.count():
        print("count matches")
    insuredatarepart=insuredatamerged.repartition(8)
    rdd_20191001=insuredatarepart.filter(lambda p:p[2]=="01-10-2019")
    print("count11111",rdd_20191001.count())
    rdd_20191002=insuredatarepart.filter(lambda p:p[2]=="02-10-2019")
    print(rdd_20191002.count())
# rdd_20191001.saveAsTextFile("hdfs:///user/hduser/sparkhack2/rdd_20191001")
# rdd_20191001.saveAsTextFile("hdfs:///user/hduser/sparkhack2/rdd_20191002")
    insuredaterepartdf=insuredatarepart.toDF()
    print(insuredaterepartdf.show(2874))

