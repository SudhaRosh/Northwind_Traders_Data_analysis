# Databricks notebook source
# /FileStore/tables/Suppliers.csv
# /FileStore/tables/Products.csv
# /FileStore/tables/Employees.csv
# /FileStore/tables/Orders.csv
# /FileStore/tables/Customers.csv

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import date_diff, col


# COMMAND ----------

product_path='/FileStore/tables/Products.csv'
product_df=spark.read.format('csv').load(product_path,inferSchema='True',header='True')

# COMMAND ----------

order_path='/FileStore/tables/Orders.csv'
order_df=spark.read.format('csv').load(order_path,inferSchema='True',header='True')

# COMMAND ----------

Employees_path='/FileStore/tables/Employees.csv'
Employees_df=spark.read.format('csv').load(Employees_path,inferSchema='True',header='True')

# COMMAND ----------

Customers_path='/FileStore/tables/Customers.csv'
Customers_df=spark.read.format('csv').load(Customers_path,inferSchema='True',header='True')

# COMMAND ----------

Suppliers_path='/FileStore/tables/Suppliers.csv'
Suppliers_df=spark.read.format('csv').load(Suppliers_path,inferSchema='True',header='True')

# COMMAND ----------

# DBTITLE 1,Question 1
# MAGIC %md
# MAGIC ### 1. How many current products cost less than $20?
# MAGIC ### 

# COMMAND ----------

product_df.createOrReplaceTempView('product_df')

# COMMAND ----------

# DBTITLE 1,pyspark
product_df.filter((col("UnitPrice")<20) & (col("Discontinued")==False)).count()

# COMMAND ----------

# DBTITLE 1,sql
# MAGIC %sql select count(ProductID) from product_df where UnitPrice <20 and Discontinued='false' 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2  Which product is most expensive?

# COMMAND ----------

# DBTITLE 1,sql
# MAGIC %sql select ProductName,UnitPrice from product_df  order by UnitPrice desc limit 1

# COMMAND ----------

# DBTITLE 1,spark
product_df.sort(desc('UnitPrice')).limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. What is average unit price for our products

# COMMAND ----------

product_df.agg(round(avg('UnitPrice'),2).alias('AvgUnitPrice')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. How many products are above the average unit price?
# MAGIC ### 

# COMMAND ----------

AvgUnitPrice=product_df.agg(round(avg('UnitPrice'),2).alias('AvgUnitPrice')).collect()[0]['AvgUnitPrice']
print(AvgUnitPrice)
product_df.filter(col('UnitPrice')>AvgUnitPrice).display()

# COMMAND ----------

AvgUnitPrice=28.87
product_df.select('ProductName').where(col('UnitPrice')>AvgUnitPrice).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. How many products cost between 15 and 25 (inclusive)?

# COMMAND ----------

product_df.filter((col('UnitPrice')>=15) & (col('UnitPrice')<=25)).display()

# COMMAND ----------

product_df.select('ProductName','UnitPrice').where((col('UnitPrice')>= 15) & (col('UnitPrice')<=25)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. What is the average number of products (not qty) per order?
# MAGIC ### 

# COMMAND ----------

order_df.display()

# COMMAND ----------

TotalProductcount=order_df.groupby('OrderID').agg(count('ProductID').alias('TotalProductcount'))

# COMMAND ----------

TotalProductcount.agg(round(avg('TotalProductcount'),0).alias('AvgProductCount')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. What is Order Value in $ of open orders? (Not Shipped Yet)
# MAGIC ### 

# COMMAND ----------

notshipped=order_df.filter(col('ShippedDate').isNull())

# COMMAND ----------

notshipped.agg(round(sum('UnitPrice').alias('TotalPrice'),2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. How many orders are 'single item' (only one product ordered)?

# COMMAND ----------

TotalProduct= order_df.groupBy('OrderID').agg(count('ProductID').alias('ProductID'))


# COMMAND ----------

TotalProduct.select('OrderID','ProductID').where(col('ProductID')==1).count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### #9. Average Sales per Transaction (orderID) for "Romero y tomillo"
# MAGIC ### 

# COMMAND ----------

order_df.display()

# COMMAND ----------

FilterShipname=order_df.filter(col('ShipName')=='Romero y tomillo')

# COMMAND ----------

TotalSumofSale=FilterShipname.groupBy('OrderID').agg(sum('_Sales').alias('Total_Sale'))

# COMMAND ----------

TotalSumofSale.agg(round(avg('Total_Sale'),2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. How many days since "North/South" last purchase?
# MAGIC

# COMMAND ----------

#join customer & order on customer_id
#or
# save customer_id from cutomer table and use customerid to filter orders  



# COMMAND ----------

Customers_df.display()

# COMMAND ----------

customer_id=Customers_df.filter(col('CompanyName')=='North/South').select('CustomerID').collect()[0]['CustomerID']
customer_id

# COMMAND ----------

norts_order=order_df.filter(col('customerID')==customer_id)

# COMMAND ----------

orderDate=norts_order.sort(desc(col('OrderDate'))).limit(1).select('CustomerID','OrderDate')

# COMMAND ----------

current_date=orderDate.withColumn('Current_Date',current_date())

# COMMAND ----------

current_date.withColumn('DateDifference',datediff(col('Current_Date'),col('OrderDate'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. How many customers have ordered only once?
# MAGIC ### 

# COMMAND ----------

productlevel=order_df.groupBy('CustomerID','OrderID').agg(count('ProductID').alias('ProductIDcount'))
#find how many times every customer has ordered 
orderlevel=productlevel.groupBy('CustomerID').agg(count('OrderID').alias('ordercount'))
orderlevel.filter(col('ordercount')==1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. How many new customers in 2023?

# COMMAND ----------

orderdata=order_df.select('OrderID','CustomerID','OrderDate').distinct()
window=Window.partitionBy('CustomerID').orderBy('OrderDate')
orderRanks=orderdata.withColumn('OrderRank',rank().over(window))


# COMMAND ----------

orderRanks.filter((col('OrderRank')==1) & (year(col('OrderDate'))==2023)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. How many Lost Customers in 2023?
# MAGIC ### 

# COMMAND ----------

orderdata=order_df.select('OrderID','CustomerID','OrderDate').distinct()
window=Window.partitionBy('CustomerID').orderBy(desc(col('OrderDate')))
orderRanks=orderdata.withColumn('OrderRank',rank().over(window))

orderRanks.filter((col('OrderRank')==1) &  (year(col('OrderDate'))==2023)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ### 13. How many customers have never purchased  Queso Cabrales?
# MAGIC

# COMMAND ----------

#product =Queso Cabrales
#join products , orders table on productid 
customer_purchased=order_df.join(product_df,on='ProductID',how='inner')\
    .filter(col('ProductName')=='Queso Cabrales')\
    .select('customerID').distinct()

# COMMAND ----------

Customers_df.join(customer_purchased,on='CustomerID',how='left_anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. How many customers have purchased only Queso Cabrales (per OrderID)?
# MAGIC ### 
# MAGIC

# COMMAND ----------

productCountOne=order_df.groupBy('orderID','customerID').agg(count('ProductID').alias('product_cnt'))\
    .filter(col('product_cnt')==1).select('customerID').distinct()

# COMMAND ----------

customers_with_product=order_df.join(product_df,on='ProductID',how='inner')\
    .filter(col('productName')=='Queso Cabrales')\
        .select('customerID').distinct()


# COMMAND ----------

productCountOne.join(customers_with_product,on='CustomerID',how='inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16 How many products are out of stock

# COMMAND ----------

product_df.display()

# COMMAND ----------

#unitinstock
#unitonorders
#Reorderlevel--threshold
product_df.filter(col('UnitsInStock')==0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 17. how many products need to be restocked(based on restock level)

# COMMAND ----------

product_df.filter(col('UnitsInStock')<col('ReorderLevel')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 18. how many products need to be [restocked](url)

# COMMAND ----------

product_df.filter((col('UnitsInStock') < col('UnitsOnOrder')) & (col('UnitsOnOrder')>0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19 What is stocked value of discontinued product

# COMMAND ----------

product_df.filter(col('Discontinued')==True)\
    .withColumn('stockedValue',col('UnitPrice')*col('UnitsInStock')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20. Which vendor has the highest Stock value 

# COMMAND ----------

productlevel=product_df.select('ProductID','ProductName','SupplierID','UnitPrice','UnitsInStock')
Stock_level=productlevel.withColumn('StockedValue',col('UnitPrice')*col('UnitsInStock'))
HightestStock=Stock_level.groupby('SupplierID').agg(sum('StockedValue').alias('SupplyValueStock'))\
    .sort(desc('SupplyValueStock')).limit(1)
HightestStock.join(Suppliers_df,on='supplierID',how='inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21. How many employees are female

# COMMAND ----------

Employees_df.display()

# COMMAND ----------

Employees_df.filter(col('Gender')=='Female').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  22. How many employee are 60 years old or above 

# COMMAND ----------


# Employees_df.select('EmployeeID','BirthDate')\
#     .withColumn('CurrentDate',current_date())\
#         .withColumn("Age", round((date_diff(col('CurrentDate'),col('BirthDate'))/365.25),2)).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #23. Which Employee has the highest sale in 2022

# COMMAND ----------

order_2022=order_df.filter(year(col('orderDate'))==2022)
highestSales=order_2022.groupby('EmployeeID').agg(round(sum('_sales'),2).alias('SalesAmt'))\
    .sort(desc('SalesAmt')).limit(1)
highestSales.join(Employees_df,on='EmployeeID',how='inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 24. How Many Employee sold over $100k in 2023

# COMMAND ----------

order_2023=order_df.filter(year(col('orderDate'))==2023)
order_2023=order_2023.groupby('EmployeeID').agg(round(sum('_sales'),2).alias('SalesAmt'))
order_2023.filter(col('SalesAmt')==100).display()

# COMMAND ----------

