// Import necessary SparkSession and DataFrame-related classes
import org.apache.spark.sql.{SparkSession, functions}

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

// Filter to include only items belonging to the "Clothing" category for men
val menClothingData = df.filter("Category = 'Clothing' AND Gender = 'Male'")
menClothingData.show()
// Filter to include only items belonging to the "Footwear" category for women
val womenFootwearData = df.filter("Category = 'Footwear' AND Gender = 'Female'")

// Perform gender-wise analysis for each item purchased in the "Clothing" category for men
val menGenderWiseAnalysis = menClothingData.groupBy("Item_Purchased", "Color")
  .agg(
    functions.count("Customer_ID").alias("Count"),
    functions.avg(functions.expr("CASE WHEN `Review_Rating` > 3.5 THEN `Review_Rating` END")).alias("AvgRating")
  )
  .filter("AvgRating is not null")
  .orderBy(functions.desc("Count"), functions.desc("AvgRating"))

menGenderWiseAnalysis.show()

// Perform gender-wise analysis for each item purchased in the "Footwear" category for women
val womenGenderWiseAnalysis = womenFootwearData.groupBy("Item_Purchased", "Color")
  .agg(
    functions.count("Customer_ID").alias("Count"),
    functions.avg(functions.expr("CASE WHEN `Review_Rating` > 3.5 THEN `Review_Rating` END")).alias("AvgRating")
  )
  .filter("AvgRating is not null")
  .orderBy(functions.desc("Count"), functions.desc("AvgRating"))

// Display the result for men
println("Gender-wise analysis for each item purchased in the Clothing category (Men):")
menGenderWiseAnalysis.show()

// Display the result for women
println("Gender-wise analysis for each item purchased in the Footwear category (Women):")
womenGenderWiseAnalysis.show()

// Count the most liked top 5 colors for each item purchased and gender in the "Clothing" category for men
val topColorsMen = menGenderWiseAnalysis.groupBy("Item_Purchased")
  .agg(
    functions.collect_list("Color").alias("Colors"),
    functions.avg("AvgRating").alias("AvgRating"),
    functions.avg("Count").alias("Count")
  )
  .withColumn("TopColors", functions.expr("slice(sort_array(Colors, false), 1, 3)"))
  .select("Item_Purchased", "TopColors", "Count", "AvgRating")
  .orderBy(functions.desc("Count"), functions.desc("AvgRating"))

// Display the result for men
println("\nTop 5 most liked colors for each item purchased in the Clothing category (Men):")
topColorsMen.show(truncate = false)

// Count the most liked top 5 colors for each item purchased and gender in the "Footwear" category for women
val topColorsWomen = womenGenderWiseAnalysis.groupBy("Item_Purchased")
  .agg(
    functions.collect_list("Color").alias("Colors"),
    functions.avg("AvgRating").alias("AvgRating"),
    functions.avg("Count").alias("Count")
  )
  .withColumn("TopColors", functions.expr("slice(sort_array(Colors, false), 1, 3)"))
  .select("Item_Purchased", "TopColors", "Count", "AvgRating")
  .orderBy(functions.desc("Count"), functions.desc("AvgRating"))

// Display the result for women
println("\nTop 5 most liked colors for each item purchased in the Footwear categor" +
  "y (Women):")
topColorsWomen.show(truncate = false)

// Stop the Spark session
spark.stop()