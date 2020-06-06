 |-- Region: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Item Type: string (nullable = true)
 |-- Sales Channel: string (nullable = true)
 |-- Order Priority: string (nullable = true)
 |-- Order Date: string (nullable = true)
 |-- Order ID: string (nullable = true)
 |-- Ship Date: string (nullable = true)
 |-- Units Sold: string (nullable = true)
 |-- Unit Price: string (nullable = true)
 |-- Unit Cost: string (nullable = true)
 |-- Total Revenue: string (nullable = true)
 |-- Total Cost: string (nullable = true)
 |-- Total Profit: string (nullable = true)


var res1 = spark.read.option("format","csv").option("header","true").csv("Downloads/Records.csv")
res1.columns.foldLeft(res1){(x,y) => { if(y.startsWith("Total")) x.withColumn(y,col(y)-1000000) else x.withColumn(y,col(y))}}
