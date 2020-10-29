# joins
ords = sc.textFile("/user/cloudera/retail_db/orders")
ordsI = sc.textFile("/user/cloudera/retail_db/order_items")

ordsM = ords.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
ordsIM = ordsI.map(lambda oi: (int(oi.split(",")[1]), oi.split(",")[4]))

ordsJ = ordsM.join(ordsIM)	# inner

ordsLOJ = ordsM.leftOuterJoin(ordsIM)	# left-outer (left is parent)
ordsROJ = ordsIM.rightOuterJoin(ordsM)	# left-outer (right is parent)
# ordsLOJ == ordsROJ 

# Full Outer Join:
#  A.fullOuterJoin(B) == (A.leftOuterJoin(B)).join(A.rightOUterJoin(B))

