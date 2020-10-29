# Aggregations - total
ordsI = sc.textFile("/user/cloudera/retail_db/order_items")
ordsI.count()

# Aggregations - total- Get revenue for given id (field [1])
ordsIF = ordsI.filter(lambda oi: int(oi.split(",")[1]) == 2)
ordsST = ordsIF.map(lambda oi: float(oi.split(",")[4]))
ordsT = ordsST.reduce(lambda x, y: x + y)
ordsMax = ordsST.reduce(lambda x, y: x if x >= y else y)
# ordsST.reduce(lambda x, y: max(x, y))

# Get count by status
ords = sc.textFile("/user/cloudera/retail_db/orders")
ordsS = ords.map(lambda o: (o.split(",")[3], 1))
ordsS.countByKey()

# Get order item details in descending order by revenue (groupByKey) for each order_id
ordsI = sc.textFile("/user/cloudera/retail_db/order_items")
ordsIM = ordsI.map(lambda oi: (float(oi.split(",")[1]), oi))
ordsIMG = ordsIM.groupByKey()
ordsIMG.flatMap(lambda oi: sorted(oi[1], key=lambda k: float(k.split(",")[4]), reverse=True))


