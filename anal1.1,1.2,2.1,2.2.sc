
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
val df = spark.read.jdbc(url, "shopping ", properties).na.drop()

val x = df.count()


/// Assuming df is the initial DataFrame
// Group by Season and calculate statistics
val purchaseStatsBySeason = df.groupBy("Season")
  .agg(
    mean("Purchase_Amount_USD").alias("MeanPurchaseAmount"),
    stddev("Purchase_Amount_USD").alias("PurchaseAmountVariation"),
    min("Purchase_Amount_USD").alias("MinPurchaseAmount"),
    max("Purchase_Amount_USD").alias("MaxPurchaseAmount")
  )
  .orderBy("Season")

println("Purchase Amount Statistics by Season:")
purchaseStatsBySeason.show()

val totalRecords = df.count()


val purchaseContributionBySubscription = df.groupBy("Subscription_Status")
  .agg(
    sum(when(col("Subscription_Status") === "Yes", 1.0).otherwise(0.0) / totalRecords * 100 +
      when(col("Subscription_Status") === "No", 1.0).otherwise(0.0) / totalRecords * 100).alias("ContributionPercentage")
  )
  .orderBy("Subscription_Status")

println("Purchase Amount Variation by Subscription Status:")
purchaseContributionBySubscription.show()

val outliersBySeason = df.groupBy("Season")
  .agg(
    expr("percentile_approx(`Purchase_Amount_USD`, 0.25)").alias("Q1"),
    expr("percentile_approx(`Purchase_Amount_USD`, 0.75)").alias("Q3"))
  .withColumn("IQR", col("Q3") - col("Q1"))
  .withColumn("LowerBound", expr("Q1 - 1.5 * IQR"))  // Corrected the calculation
  .withColumn("UpperBound", expr("Q3 + 1.5 * IQR"))

println("Outliers Detection:")
outliersBySeason.show()

// Unpersist the DataFrame when done to release resources
df.unpersist()
// Stop the Spark session


// Repartition and sort within partitions
val repartitionedDF = df.repartition(col("Location")).sortWithinPartitions(col("Purchase_Amount_USD"))

// Persist the repartitioned DataFrame
repartitionedDF.persist()

// Total Purchase Amount by Location (Top 5)
val top5PurchaseByLocation = repartitionedDF.groupBy("Location")
  .agg(sum("Purchase_Amount_USD").alias("TotalPurchaseAmount"))
  .orderBy(desc("TotalPurchaseAmount"))
  .limit(5)

println("Top 5 Purchase Amounts by Location:")
top5PurchaseByLocation.show()

// Average Purchase Amount by Location
val avgPurchaseByLocation = repartitionedDF.groupBy("Location")
  .agg(avg("Purchase_Amount_USD").alias("AvgPurchaseAmount"))
  .orderBy(desc("Location"))
  .limit(5)

println("Average Purchase Amount by Location:")
avgPurchaseByLocation.show()

// Record Count by Location
val recordCountByLocation = repartitionedDF.groupBy("Location")
  .agg(count("*").alias("RecordCount"))
  .orderBy("Location")

println("Record Count by Location:")
recordCountByLocation.show()

// Unpersist the DataFrame when done to release resources
repartitionedDF.unpersist()

val outputPath1 = "/home/karthik/Downloads/outputs/result_1.csv"

// Write the groupedResults DataFrame to a CSV file for the first output
top5PurchaseByLocation.write
  .option("header", "true")
  .csv(outputPath1)

val outputPath2 = "/home/karthik/Downloads/outputs/result_2.csv"

// Write the groupedResults DataFrame to a CSV file for the first output
avgPurchaseByLocation.write
  .option("header", "true")
  .csv(outputPath2)

val outputPath3 = "/home/karthik/Downloads/outputs/result_3.csv"

// Write the groupedResults DataFrame to a CSV file for the first output
recordCountByLocation.write
  .option("header", "true")
  .csv(outputPath3)

// Stop the Spark session

spark.stop()