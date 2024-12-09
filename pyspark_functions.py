1) how to import the data using pyspark

from pyspark.sql.functions import*
df=spark.read.csv("dbfs:/FileStore/shared_uploads/kiranc9901@gmail.com/Orders.csv",header=True,inferSchema=True)
display(df)

#display is databricks function

from pyspark.sql.functions import*
df=spark.read.csv("dbfs:/FileStore/shared_uploads/kiranc9901@gmail.com/Orders.csv",header=True,inferSchema=True)
df.show()

#df.show() spark functions which shows only first 20 rows 

output:
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|              Region|   Country|       ItemType|SalesChannel|OrderPriority|  OrderID|UnitsSold|UnitPrice|UnitCost|TotalRevenue| TotalCost|TotalProfit|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|Middle East and N...|     Libya|      Cosmetics|     Offline|            M|686800706|     8446|    437.2|  263.33|   3692591.2|2224085.18| 1468506.02|
|       North America|    Canada|     Vegetables|      Online|            M|185941302|     3018|   154.06|   90.93|   464953.08| 274426.74|  190526.34|
|Middle East and N...|     Libya|      Baby Food|     Offline|            C|246222341|     1517|   255.28|  159.42|   387259.76| 241840.14|  145419.62|
|                Asia|     Japan|         Cereal|     Offline|            C|161442649|     3322|    205.7|  117.11|    683335.4| 389039.42|  294295.98|
|  Sub-Saharan Africa|      Chad|         Fruits|     Offline|            H|645713555|     9845|     9.33|    6.92|    91853.85|   68127.4|   23726.45|
|              Europe|   Armenia|         Cereal|      Online|            H|683458888|     9528|    205.7|  117.11|   1959909.6|1115824.08|  844085.52|
|  Sub-Saharan Africa|   Eritrea|         Cereal|      Online|            H|679414975|     2844|    205.7|  117.11|    585010.8| 333060.84|  251949.96|
|              Europe|Montenegro|        Clothes|     Offline|            M|208630645|     7299|   109.28|   35.84|   797634.72| 261596.16|  536038.56|
|Central America a...|   Jamaica|     Vegetables|      Online|            H|266467225|     2428|   154.06|   90.93|   374057.68| 220778.04|  153279.64|
|Australia and Oce...|      Fiji|     Vegetables|     Offline|            H|118598544|     4800|   154.06|   90.93|    739488.0|  436464.0|   303024.0|
|  Sub-Saharan Africa|      Togo|        Clothes|      Online|            M|451010930|     3012|   109.28|   35.84|   329151.36| 107950.08|  221201.28|
|              Europe|Montenegro|         Snacks|     Offline|            M|220003211|     2694|   152.58|   97.44|   411050.52| 262503.36|  148547.16|
|              Europe|    Greece|      Household|      Online|            C|702186715|     1508|   668.27|  502.54|  1007751.16| 757830.32|  249920.84|
|  Sub-Saharan Africa|     Sudan|      Cosmetics|      Online|            C|544485270|     4146|    437.2|  263.33|   1812631.2|1091766.18|  720865.02|
|                Asia|  Maldives|         Fruits|     Offline|            L|714135205|     7332|     9.33|    6.92|    68407.56|  50737.44|   17670.12|
|              Europe|Montenegro|        Clothes|     Offline|            H|448685348|     4820|   109.28|   35.84|    526729.6|  172748.8|   353980.8|
|              Europe|   Estonia|Office Supplies|      Online|            H|405997025|     2397|   651.21|  524.96|  1560950.37|1258329.12|  302621.25|
|       North America| Greenland|      Beverages|      Online|            M|414244067|     2880|    47.45|   31.79|    136656.0|   91555.2|    45100.8|
|  Sub-Saharan Africa|Cape Verde|        Clothes|      Online|            C|821912801|     1117|   109.28|   35.84|   122065.76|  40033.28|   82032.48|
|  Sub-Saharan Africa|   Senegal|      Household|     Offline|            L|247802054|     8989|   668.27|  502.54|  6007079.03|4517332.06| 1489746.97|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
only showing top 20 rows


2) Select only the 'OrderID', 'Region', and 'TotalRevenue' columns from the dataset.

df2=df.select("OrderId","Region","TotalRevenue")
df2.show()

output:
+---------+--------------------+------------+
|  OrderId|              Region|TotalRevenue|
+---------+--------------------+------------+
|686800706|Middle East and N...|   3692591.2|
|185941302|       North America|   464953.08|
|246222341|Middle East and N...|   387259.76|
|161442649|                Asia|    683335.4|
|645713555|  Sub-Saharan Africa|    91853.85|
|683458888|              Europe|   1959909.6|
|679414975|  Sub-Saharan Africa|    585010.8|
|208630645|              Europe|   797634.72|
|266467225|Central America a...|   374057.68|
|118598544|Australia and Oce...|    739488.0|
|451010930|  Sub-Saharan Africa|   329151.36|
|220003211|              Europe|   411050.52|
|702186715|              Europe|  1007751.16|
|544485270|  Sub-Saharan Africa|   1812631.2|
|714135205|                Asia|    68407.56|
|448685348|              Europe|    526729.6|
|405997025|              Europe|  1560950.37|
|414244067|       North America|    136656.0|
|821912801|  Sub-Saharan Africa|   122065.76|
|247802054|  Sub-Saharan Africa|  6007079.03|
+---------+--------------------+------------+

3)Find all orders from the 'Europe' region with a total revenue greater than $500,000.

df3=df.filter((col("Region")=="Europe") & (col("TotalRevenue")>500000))
df3.show()

output:
+------+--------------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|Region|       Country|       ItemType|SalesChannel|OrderPriority|  OrderID|UnitsSold|UnitPrice|UnitCost|TotalRevenue| TotalCost|TotalProfit|
+------+--------------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|Europe|       Armenia|         Cereal|      Online|            H|683458888|     9528|    205.7|  117.11|   1959909.6|1115824.08|  844085.52|
|Europe|    Montenegro|        Clothes|     Offline|            M|208630645|     7299|   109.28|   35.84|   797634.72| 261596.16|  536038.56|
|Europe|        Greece|      Household|      Online|            C|702186715|     1508|   668.27|  502.54|  1007751.16| 757830.32|  249920.84|
|Europe|    Montenegro|        Clothes|     Offline|            H|448685348|     4820|   109.28|   35.84|    526729.6|  172748.8|   353980.8|
|Europe|       Estonia|Office Supplies|      Online|            H|405997025|     2397|   651.21|  524.96|  1560950.37|1258329.12|  302621.25|
|Europe|      Bulgaria|        Clothes|      Online|            L|880999934|     6313|   109.28|   35.84|   689884.64| 226257.92|  463626.72|
|Europe|        Greece|      Baby Food|     Offline|            M|294499957|     7937|   255.28|  159.42|  2026157.36|1265316.54|  760840.82|
|Europe|        Sweden|      Baby Food|      Online|            L|689975583|     7963|   255.28|  159.42|  2032794.64|1269461.46|  763333.18|
|Europe|       Belarus|Office Supplies|      Online|            L|759279143|     6426|   651.21|  524.96|  4184675.46|3373392.96|   811282.5|
|Europe|       Armenia|           Meat|      Online|            C|489148938|     8896|   421.89|  364.69|  3753133.44|3244282.24|   508851.2|
|Europe|        Greece|      Household|      Online|            L|876286971|     1643|   668.27|  502.54|  1097967.61| 825673.22|  272294.39|
|Europe|       Ukraine|      Cosmetics|      Online|            M|270001733|     8368|    437.2|  263.33|   3658489.6|2203545.44| 1454944.16|
|Europe|         Italy|Office Supplies|      Online|            M|812295901|     5263|   651.21|  524.96|  3427318.23|2762864.48|  664453.75|
|Europe|      Portugal|Office Supplies|      Online|            L|535654580|      949|   651.21|  524.96|   617998.29| 498187.04|  119811.25|
|Europe|       Romania|Office Supplies|     Offline|            C|810871112|     3636|   651.21|  524.96|  2367799.56|1908754.56|   459045.0|
|Europe|       Austria|Office Supplies|      Online|            L|285341823|     7841|   651.21|  524.96|  5106137.61|4116211.36|  989926.25|
|Europe|    Luxembourg|      Baby Food|     Offline|            L|817740142|     6335|   255.28|  159.42|   1617198.8| 1009925.7|   607273.1|
|Europe|        Sweden|     Vegetables|     Offline|            M|947434604|     5808|   154.06|   90.93|   894780.48| 528121.44|  366659.04|
|Europe|       Iceland|           Meat|     Offline|            H|869397771|     2975|   421.89|  364.69|  1255122.75|1084952.75|   170170.0|
|Europe|United Kingdom|Office Supplies|      Online|            M|350274455|     2850|   651.21|  524.96|   1855948.5| 1496136.0|   359812.5|
+------+--------------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
only showing top 20 rows

4) List the distinct item types sold in the 'Middle East and North Africa' region.

df4=df.filter(col("Region")=="Middle East and North Africa").select("ItemType").distinct()
df4.show()

output:
+---------------+
|       ItemType|
+---------------+
|      Baby Food|
|         Cereal|
|           Meat|
|      Household|
|     Vegetables|
|      Beverages|
|Office Supplies|
|      Cosmetics|
|  Personal Care|
|         Fruits|
|         Snacks|
|        Clothes|
+---------------+

5) Select orders that are either 'High' priority or have a total profit greater than $100,000.

df5=df.filter((col("OrderPriority")=="H") | (col("TotalProfit") > 100000))
display(df5)

output:
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|              Region|   Country|       ItemType|SalesChannel|OrderPriority|  OrderID|UnitsSold|UnitPrice|UnitCost|TotalRevenue| TotalCost|TotalProfit|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
|Middle East and N...|     Libya|      Cosmetics|     Offline|            M|686800706|     8446|    437.2|  263.33|   3692591.2|2224085.18| 1468506.02|
|       North America|    Canada|     Vegetables|      Online|            M|185941302|     3018|   154.06|   90.93|   464953.08| 274426.74|  190526.34|
|Middle East and N...|     Libya|      Baby Food|     Offline|            C|246222341|     1517|   255.28|  159.42|   387259.76| 241840.14|  145419.62|
|                Asia|     Japan|         Cereal|     Offline|            C|161442649|     3322|    205.7|  117.11|    683335.4| 389039.42|  294295.98|
|  Sub-Saharan Africa|      Chad|         Fruits|     Offline|            H|645713555|     9845|     9.33|    6.92|    91853.85|   68127.4|   23726.45|
|              Europe|   Armenia|         Cereal|      Online|            H|683458888|     9528|    205.7|  117.11|   1959909.6|1115824.08|  844085.52|
|  Sub-Saharan Africa|   Eritrea|         Cereal|      Online|            H|679414975|     2844|    205.7|  117.11|    585010.8| 333060.84|  251949.96|
|              Europe|Montenegro|        Clothes|     Offline|            M|208630645|     7299|   109.28|   35.84|   797634.72| 261596.16|  536038.56|
|Central America a...|   Jamaica|     Vegetables|      Online|            H|266467225|     2428|   154.06|   90.93|   374057.68| 220778.04|  153279.64|
|Australia and Oce...|      Fiji|     Vegetables|     Offline|            H|118598544|     4800|   154.06|   90.93|    739488.0|  436464.0|   303024.0|
|  Sub-Saharan Africa|      Togo|        Clothes|      Online|            M|451010930|     3012|   109.28|   35.84|   329151.36| 107950.08|  221201.28|
|              Europe|Montenegro|         Snacks|     Offline|            M|220003211|     2694|   152.58|   97.44|   411050.52| 262503.36|  148547.16|
|              Europe|    Greece|      Household|      Online|            C|702186715|     1508|   668.27|  502.54|  1007751.16| 757830.32|  249920.84|
|  Sub-Saharan Africa|     Sudan|      Cosmetics|      Online|            C|544485270|     4146|    437.2|  263.33|   1812631.2|1091766.18|  720865.02|
|              Europe|Montenegro|        Clothes|     Offline|            H|448685348|     4820|   109.28|   35.84|    526729.6|  172748.8|   353980.8|
|              Europe|   Estonia|Office Supplies|      Online|            H|405997025|     2397|   651.21|  524.96|  1560950.37|1258329.12|  302621.25|
|  Sub-Saharan Africa|   Senegal|      Household|     Offline|            L|247802054|     8989|   668.27|  502.54|  6007079.03|4517332.06| 1489746.97|
|              Europe|  Bulgaria|        Clothes|      Online|            L|880999934|     6313|   109.28|   35.84|   689884.64| 226257.92|  463626.72|
|Middle East and N...|   Algeria|  Personal Care|      Online|            H|127468717|     9681|    81.73|   56.67|   791228.13| 548622.27|  242605.86|
|Central America a...|   Grenada|         Cereal|      Online|            H|430390107|      852|    205.7|  117.11|    175256.4|  99777.72|   75478.68|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+
only showing top 20 rows

6) Create a new column called 'RevenueCategory' that categorizes total revenue as 'Low' if less than $100,000, 'Medium' if between $100,000 and $500,000, and 'High' if greater than $500,000.

df6=df.withColumn("RevenueCategory", when(col("TotalRevenue") < 100000, "Low")
    .when((col("TotalRevenue") >= 100000) & (col("TotalRevenue") <= 500000), "Medium")
    .otherwise("High"))
df6.show()

output:
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+---------------+
|              Region|   Country|       ItemType|SalesChannel|OrderPriority|  OrderID|UnitsSold|UnitPrice|UnitCost|TotalRevenue| TotalCost|TotalProfit|RevenueCategory|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+---------------+
|Middle East and N...|     Libya|      Cosmetics|     Offline|            M|686800706|     8446|    437.2|  263.33|   3692591.2|2224085.18| 1468506.02|           High|
|       North America|    Canada|     Vegetables|      Online|            M|185941302|     3018|   154.06|   90.93|   464953.08| 274426.74|  190526.34|         Medium|
|Middle East and N...|     Libya|      Baby Food|     Offline|            C|246222341|     1517|   255.28|  159.42|   387259.76| 241840.14|  145419.62|         Medium|
|                Asia|     Japan|         Cereal|     Offline|            C|161442649|     3322|    205.7|  117.11|    683335.4| 389039.42|  294295.98|           High|
|  Sub-Saharan Africa|      Chad|         Fruits|     Offline|            H|645713555|     9845|     9.33|    6.92|    91853.85|   68127.4|   23726.45|            Low|
|              Europe|   Armenia|         Cereal|      Online|            H|683458888|     9528|    205.7|  117.11|   1959909.6|1115824.08|  844085.52|           High|
|  Sub-Saharan Africa|   Eritrea|         Cereal|      Online|            H|679414975|     2844|    205.7|  117.11|    585010.8| 333060.84|  251949.96|           High|
|              Europe|Montenegro|        Clothes|     Offline|            M|208630645|     7299|   109.28|   35.84|   797634.72| 261596.16|  536038.56|           High|
|Central America a...|   Jamaica|     Vegetables|      Online|            H|266467225|     2428|   154.06|   90.93|   374057.68| 220778.04|  153279.64|         Medium|
|Australia and Oce...|      Fiji|     Vegetables|     Offline|            H|118598544|     4800|   154.06|   90.93|    739488.0|  436464.0|   303024.0|           High|
|  Sub-Saharan Africa|      Togo|        Clothes|      Online|            M|451010930|     3012|   109.28|   35.84|   329151.36| 107950.08|  221201.28|         Medium|
|              Europe|Montenegro|         Snacks|     Offline|            M|220003211|     2694|   152.58|   97.44|   411050.52| 262503.36|  148547.16|         Medium|
|              Europe|    Greece|      Household|      Online|            C|702186715|     1508|   668.27|  502.54|  1007751.16| 757830.32|  249920.84|           High|
|  Sub-Saharan Africa|     Sudan|      Cosmetics|      Online|            C|544485270|     4146|    437.2|  263.33|   1812631.2|1091766.18|  720865.02|           High|
|                Asia|  Maldives|         Fruits|     Offline|            L|714135205|     7332|     9.33|    6.92|    68407.56|  50737.44|   17670.12|            Low|
|              Europe|Montenegro|        Clothes|     Offline|            H|448685348|     4820|   109.28|   35.84|    526729.6|  172748.8|   353980.8|           High|
|              Europe|   Estonia|Office Supplies|      Online|            H|405997025|     2397|   651.21|  524.96|  1560950.37|1258329.12|  302621.25|           High|
|       North America| Greenland|      Beverages|      Online|            M|414244067|     2880|    47.45|   31.79|    136656.0|   91555.2|    45100.8|         Medium|
|  Sub-Saharan Africa|Cape Verde|        Clothes|      Online|            C|821912801|     1117|   109.28|   35.84|   122065.76|  40033.28|   82032.48|         Medium|
|  Sub-Saharan Africa|   Senegal|      Household|     Offline|            L|247802054|     8989|   668.27|  502.54|  6007079.03|4517332.06| 1489746.97|           High|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+--------+------------+----------+-----------+---------------+
only showing top 20 rows



7)Remove the 'UnitCost' column from the dataset and rename 'TotalRevenue' to 'Revenue'.

df7=df.drop("UnitCost").withColumnRenamed("TotalRevenue", "Revenue")
df7.show()

output : 
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+----------+----------+-----------+
|              Region|   Country|       ItemType|SalesChannel|OrderPriority|  OrderID|UnitsSold|UnitPrice|   Revenue| TotalCost|TotalProfit|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+----------+----------+-----------+
|Middle East and N...|     Libya|      Cosmetics|     Offline|            M|686800706|     8446|    437.2| 3692591.2|2224085.18| 1468506.02|
|       North America|    Canada|     Vegetables|      Online|            M|185941302|     3018|   154.06| 464953.08| 274426.74|  190526.34|
|Middle East and N...|     Libya|      Baby Food|     Offline|            C|246222341|     1517|   255.28| 387259.76| 241840.14|  145419.62|
|                Asia|     Japan|         Cereal|     Offline|            C|161442649|     3322|    205.7|  683335.4| 389039.42|  294295.98|
|  Sub-Saharan Africa|      Chad|         Fruits|     Offline|            H|645713555|     9845|     9.33|  91853.85|   68127.4|   23726.45|
|              Europe|   Armenia|         Cereal|      Online|            H|683458888|     9528|    205.7| 1959909.6|1115824.08|  844085.52|
|  Sub-Saharan Africa|   Eritrea|         Cereal|      Online|            H|679414975|     2844|    205.7|  585010.8| 333060.84|  251949.96|
|              Europe|Montenegro|        Clothes|     Offline|            M|208630645|     7299|   109.28| 797634.72| 261596.16|  536038.56|
|Central America a...|   Jamaica|     Vegetables|      Online|            H|266467225|     2428|   154.06| 374057.68| 220778.04|  153279.64|
|Australia and Oce...|      Fiji|     Vegetables|     Offline|            H|118598544|     4800|   154.06|  739488.0|  436464.0|   303024.0|
|  Sub-Saharan Africa|      Togo|        Clothes|      Online|            M|451010930|     3012|   109.28| 329151.36| 107950.08|  221201.28|
|              Europe|Montenegro|         Snacks|     Offline|            M|220003211|     2694|   152.58| 411050.52| 262503.36|  148547.16|
|              Europe|    Greece|      Household|      Online|            C|702186715|     1508|   668.27|1007751.16| 757830.32|  249920.84|
|  Sub-Saharan Africa|     Sudan|      Cosmetics|      Online|            C|544485270|     4146|    437.2| 1812631.2|1091766.18|  720865.02|
|                Asia|  Maldives|         Fruits|     Offline|            L|714135205|     7332|     9.33|  68407.56|  50737.44|   17670.12|
|              Europe|Montenegro|        Clothes|     Offline|            H|448685348|     4820|   109.28|  526729.6|  172748.8|   353980.8|
|              Europe|   Estonia|Office Supplies|      Online|            H|405997025|     2397|   651.21|1560950.37|1258329.12|  302621.25|
|       North America| Greenland|      Beverages|      Online|            M|414244067|     2880|    47.45|  136656.0|   91555.2|    45100.8|
|  Sub-Saharan Africa|Cape Verde|        Clothes|      Online|            C|821912801|     1117|   109.28| 122065.76|  40033.28|   82032.48|
|  Sub-Saharan Africa|   Senegal|      Household|     Offline|            L|247802054|     8989|   668.27|6007079.03|4517332.06| 1489746.97|
+--------------------+----------+---------------+------------+-------------+---------+---------+---------+----------+----------+-----------+
only showing top 20 rows



