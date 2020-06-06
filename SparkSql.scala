res1.columns.foldLeft(res1){(x,y) => { if(y.startsWith("Total")) x.withColumn(y,col(y)-1000000) else x.withColumn(y,col(y))}}
