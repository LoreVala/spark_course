# Sort data by product price
products = sc.textFile("/user/cloudera/retail_db/products")
for i in products.take(10): print(i)

productsM = products.map(lambda p: (float(p.split(",")[4]), p))

sortp = productsM.sortByKey()	# Ascending
sortp = productsM.sortByKey(False)	# Descending

