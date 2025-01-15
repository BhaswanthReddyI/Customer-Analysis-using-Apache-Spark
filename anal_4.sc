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
val df = spark.read.jdbc(url, "shopping ", properties)

// Add a new column for total revenue for each row
val dfWithTotalRevenue = df.withColumn("TotalRevenue", col("Purchase_Amount_USD").cast("double"))

// Group by season, category, and location; calculate the total revenue and the purchased item for each category within each season and location
val seasonalCategoryLocationTotalRevenue = dfWithTotalRevenue.groupBy("Season", "Category", "Location")
  .agg(
    sum("TotalRevenue").alias("TotalCategoryLocationRevenue"),
    max("Item_Purchased").alias("TopItem")
  )

// Identify the location that contributed the most revenue and the purchased item for each season and category
val topLocationPerSeasonAndCategory = seasonalCategoryLocationTotalRevenue
  .orderBy(desc("TotalCategoryLocationRevenue"))
  .groupBy("Season", "Category")
  .agg(
    first("Location").alias("TopLocation"),
    sum("TotalCategoryLocationRevenue").alias("CategoryRevenue"),  // Change here to get category revenue
    first("TopItem").alias("TopItem")
  )

// Show the location that contributed the most revenue and the purchased item for each season and category
println("Location with the Most Revenue Contribution and Top Purchased Item for Each Season and Category:")
topLocationPerSeasonAndCategory.show()

// Stop the Spark session
spark.stop()