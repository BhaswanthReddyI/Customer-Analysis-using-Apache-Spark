

// Import necessary SparkSession and DataFrame-related classes
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
val spark = SparkSession.builder()
  .appName("JDBC Spark Example")
  .config("spark.master", "local")
  .getOrCreate()

// Database connection properties
val url = "jdbc:mysql://localhost:3306/abhinav"
val properties = new java.util.Properties()
properties.setProperty("user", "root")
properties.setProperty("password", "2003")

// Read data from the database table using JDBC
val df = spark.read.jdbc(url, "shopping", properties)

// Define the conditions for customers who have not subscribed
val conditions = col("Subscription_Status") === "No" && col("Previous_Purchases").cast("int") > 15 && col("Review_Rating").cast("double") > 3.5

// Apply the conditions and add a new column 'message need to be delivered' based on 'Frequency of Purchases'
val resultDF = df
  .withColumn("Customer_ID", col("Customer_ID") + 1) // Increment Customer ID by 1
  .withColumn("message need to be delivered", when(conditions && col("Frequency_of_Purchases") === "Weekly", lit("Save upto 10% via primeo subscription"))
    .when(conditions && col("Frequency_of_Purchases") === "Annually", lit("Exciting offers being unlocked this new year!!"))
    .when(conditions && col("Frequency_of_Purchases") === "Bi-Weekly", lit("Buy 1 Get 1 Free with Primeo Subscription"))
    .when(conditions && col("Frequency_of_Purchases") === "Quarterly", lit("Festival Sale Starting Soon"))
    .when(conditions && col("Frequency_of_Purchases") === "Every 3 Months", lit("Flat 40% OFF "))
    .when(conditions && col("Frequency_of_Purchases") === "Fortnightly", lit("Great Deals unlocking this week"))
    .when(conditions && col("Frequency_of_Purchases") === "Monthly", lit("New Stock Alert"))

    .otherwise(lit(null)))
  .filter(conditions) // Filter only customers who have not subscribed
  .select("Customer_ID", "Contact_No", "message need to be delivered") // Corrected the column names

// Show the result
resultDF.show(false)

// Stop the Spark session
spark.stop()