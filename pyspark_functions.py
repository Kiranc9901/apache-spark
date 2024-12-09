import the data using pyspark

df=spark.read.csv("/FileStore/shared_uploads/kiranc9901@gmail.com",header=True,inferSchema=True)
display(df)

#display is databricks function

from pyspark.sql.functions import*
df=spark.read.csv("/FileStore/shared_uploads/kiranc9901@gmail.com",header=True,inferSchema=True)
df.show()

#df.show() spark functions which shows only first 20 rows 

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




