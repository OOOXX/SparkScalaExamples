val accum = sc.longAccumulator("My Accumulator")
//accum: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 0, name: Some(My Accumulator), value: 0)

sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))

//10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

accum.value
//res2: Long = 10
