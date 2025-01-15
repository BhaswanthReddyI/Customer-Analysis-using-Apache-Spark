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
// Define age groups
val ageGroups = Array((20, 30), (30, 40), (40, 50), (50, 60), (60, 70))

// Add new columns for age group and review range
val dfWithAgeAndReview = df
  .withColumn("AgeGroup", when(col("Age").between(20, 30), "20-30")
    .when(col("Age").between(30, 40), "30-40")
    .when(col("Age").between(40, 50), "40-50")
    .when(col("Age").between(50, 60), "50-60")
    .when(col("Age").between(60, 70), "60-70")
    .otherwise("Unknown"))
  .withColumn("ReviewRange", when(col("Review_Rating") > 4.0, "Above 4")
    .when(col("Review_Rating").between(3.0, 4.0), "3 to 4")
    .when(col("Review_Rating").between(2.0, 3.0), "2 to 3")
    .otherwise("1 to 2"))

// Group by age group, category, and review range, and count the occurrences
val reviewCountByAgeAndCategory = dfWithAgeAndReview.groupBy("AgeGroup", "Category", "ReviewRange")
  .agg(count("Customer_ID").as("ReviewCount"))
  .orderBy("AgeGroup", "Category", "ReviewRange")

// Show the result
reviewCountByAgeAndCategory.show()

// Filter the results based on additional criteria and add ReviewSentiment column
val filteredResults = reviewCountByAgeAndCategory
  .filter("(ReviewRange = 'Above 4' AND ReviewCount > 100)")
  .withColumn("ReviewSentiment",
      when(col("ReviewRange") === "Above 4", "Most Liked"))
  .select("AgeGroup", "Category", "ReviewSentiment", "ReviewCount")
  .orderBy("AgeGroup", "Category")

// Group by AgeGroup and Category and concatenate ReviewSentiment values
val groupedResults = filteredResults
  .groupBy("AgeGroup", "Category")
  .agg(
      sum("ReviewCount").as("TotalReviewCount"))
  .orderBy("AgeGroup", "Category")

// Show the grouped result
groupedResults.show()

// Specify the path where you want to save the CSV file for the first output
val outputPath1 = "/home/karthik/Downloads/outputs/Analysis_result_1.csv"

// Write the groupedResults DataFrame to a CSV file for the first output
reviewCountByAgeAndCategory.write
  .option("header", "true") // Write the header in the CSV file
  .csv(outputPath1)

// Specify the path where you want to save the CSV file for the second output
val outputPath2 = "/home/karthik/Downloads/outputs/Analysis_result_2.csv"

// Write the groupedResults DataFrame to a CSV file for the second output
groupedResults.write
  .option("header", "true") // Write the header in the CSV file
  .csv(outputPath2)

// Stop the Spark session
spark.stop()