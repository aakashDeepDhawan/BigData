import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import java.sql.Date

case class Orders(orders_id: Int, order_date: Date, order_customer_id: Int, order_status: String)
case class Products(product_id: Int, product_category_id: Int, product_name: String, product_description: String, product_price: Float, product_image: String)
case class OrderItems(order_item_id: Int, order_item_order_id: Int, order_item_product_id: Int, order_item_quantity: Int, order_item_subtotal: Float, order_item_product_price: Float)
case class Customers(customer_id: Int, customer_fname: String, customer_lname: String, customer_email: String, customer_password: String, customer_street: String, customer_city: String, customer_state: String, customer_zipcode: String)

object RetailAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Intelliment Assignment").setMaster("local[*]").set("spark.testing.memory", "471859200") //.set("spark.driver.memory", "2g")  //   2147480000
    val sc = new SparkContext(conf) // spark Context Created
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val productsFilePath = "E:\\intelliment\\assignment\\Input_dataSet\\Products.txt"
    val ordersFilePath = "E:\\intelliment\\assignment\\Input_dataSet\\Orders.txt"
    val orderItemsFilePath = "E:\\intelliment\\assignment\\Input_dataSet\\Order_Items.txt"
    val customersFilePath = "E:\\intelliment\\assignment\\Input_dataSet\\Customers.txt"

    val ordersDs = sc.textFile(ordersFilePath).map(_.split(",")).map { row => Orders(row(0).toInt, Date.valueOf(row(1)), row(2).toInt, row(3).toString) }.toDF()
    ordersDs.registerTempTable("Orders")
    ordersDs.show()

    val productsDs = sc.textFile(productsFilePath).map(_.split(";")).map { row => Products(row(0).toInt, row(1).toInt, row(2).toString, row(3).toString, row(4).toFloat, row(5).toString) }.toDF()
    productsDs.registerTempTable("Products")
    productsDs.show()

    val orderItemsDs = sc.textFile(orderItemsFilePath).map(_.split(",")).map { row => OrderItems(row(0).toInt, row(1).toInt, row(2).toInt, row(3).toInt, row(4).toFloat, row(5).toFloat) }.toDF()
    orderItemsDs.registerTempTable("Order_Items")
    orderItemsDs.show()

    val customersDs = sc.textFile(customersFilePath).map(_.split(";")).map { row => Customers(row(0).toInt, row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString, row(6).toString, row(7).toString, row(8).toString) }.toDF()
    customersDs.registerTempTable("Customers")
    customersDs.show()

    // ques1 : Total sales for each date (hint: join orders & order_items)
    val ques1 = sqlContext.sql("select ord.order_date,sum(order_item_subtotal) as totalsales from Orders ord inner join Order_Items ort where ord.orders_id=ort.order_item_order_id group by ord.order_date")
    val ques1Output = ques1.show()

    //ques2 : Total sales for each month
    val ques2 = sqlContext.sql("select YEAR(ord.order_date) as SalesYear,MONTH(ord.order_date) as SalesMonth,sum(order_item_subtotal) as totalsales from Orders ord inner join Order_Items ort where ord.orders_id=ort.order_item_order_id GROUP BY YEAR(ord.order_date),MONTH(ord.order_date) ORDER BY YEAR(ord.order_date), MONTH(ord.order_date)")
    val ques2Output = ques2.show()

    //ques3 : Average sales for each date     
    val ques3 = sqlContext.sql("select ord.order_date,avg(order_item_subtotal) as avgSales from Orders ord inner join Order_Items ort where ord.orders_id=ort.order_item_order_id group by ord.order_date")
    val ques3Output = ques3.show()

    //ques4 : Average sales for each month    
    val ques4 = sqlContext.sql("select YEAR(ord.order_date) as SalesYear,MONTH(ord.order_date) as SalesMonth,avg(order_item_subtotal) as totalsales from Orders ord inner join Order_Items ort where ord.orders_id=ort.order_item_order_id GROUP BY YEAR(ord.order_date),MONTH(ord.order_date) ORDER BY YEAR(ord.order_date), MONTH(ord.order_date)")
    val ques4Output = ques4.show()

    //ques5 : Name of Month having highest sale
    val ques5 = sqlContext.sql("select YEAR(ord.order_date) as SalesYear , MONTH(ord.order_date) as SalesMonth , sum(order_item_subtotal) as maxsalesmonth from Orders ord inner join Order_Items ort where ord.orders_id=ort.order_item_order_id GROUP BY YEAR(ord.order_date),MONTH(ord.order_date) order by sum(order_item_subtotal) desc limit 1")
    val ques5Output = ques5.show()

    //ques6 : Top 10 revenue generating products
    val ques6 = sqlContext.sql("select prd.product_id,prd.product_name,sum(prd.product_price) as revenue from Products prd inner join Order_Items ort where prd.product_id=ort.order_item_product_id group by prd.product_name,prd.product_id order by sum(ort.order_item_quantity) desc limit 10")
    val ques6Output = ques6.show()

    //ques7 : Top 3 purchased customers for each day/month
    //facing issue in selection top 3 rows tried with Row_num

    //ques8 : Most sold products for each day/month
    val ques8 = sqlContext.sql("select prd.product_name,YEAR(ord.order_date) as SalesYear,MONTH(ord.order_date) as SalesMonth,DAY(ord.order_date) as DAY,sum(ort.order_item_quantity) from Products prd inner join Order_Items ort inner join Orders ord where prd.product_id=ort.order_item_product_id and ord.orders_id=ort.order_item_order_id group by YEAR(ord.order_date),MONTH(ord.order_date),DAY(ord.order_date),prd.product_name order by sum(ort.order_item_quantity) desc")
    val ques8Output = ques8.show()

    //ques9 : Count of distinct Customer, group by State  (use customer table)
    val ques9 = sqlContext.sql("select cust.customer_state,count(cust.customer_id) as customerCount from Customers cust group by cust.customer_state")
    val ques9Output = ques9.show()

    //ques10 : Most popular product category
    val ques10 = sqlContext.sql("select prod.product_category_id from Products prod inner join Order_Items ort where prod.product_id=ort.order_item_product_id group by prod.product_category_id ORDER BY count(ort.order_item_quantity) DESC limit 1")
    val ques10Output = ques10.show()

  }
}