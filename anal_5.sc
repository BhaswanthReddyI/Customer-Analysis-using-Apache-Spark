// Import necessary SparkSession and DataFrame-related classes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.types.DoubleType

// Create a Spark session
val spark = SparkSession.builder
  .appName("LogisticRegressionExample")
  .master("local[2]")
  .getOrCreate()

// Database connection properties
val url = "jdbc:mysql://localhost:3306/abhinav"
val properties = new java.util.Properties()
properties.setProperty("user", "root")
properties.setProperty("password", "2003")

// Read data from the database table using JDBC
val df = spark.read.jdbc(url, "shopping ", properties)

// Select relevant columns
val selectedFeatures = Array("Age", "Previous_Purchases", "Purchase_Amount_USD")
val numericColumns = selectedFeatures.map(col)

// Filter out rows with null values in the selected features
val filteredDF = df.select(numericColumns: _*).na.drop()

// Add a new column for the binary label (1 if Purchase_Amount_(USD) > 50, else 0)
val dfWithLabel = filteredDF.withColumn("label", when(col("Purchase_Amount_USD") > 50, 1.0).otherwise(0.0))

// Assemble features
val assembler = new VectorAssembler()
  .setInputCols(Array("Age", "Previous_Purchases"))
  .setOutputCol("features")

// Transform the DataFrame to include the features column
val assembledDF = assembler.transform(dfWithLabel)

// Split the data into training and testing sets
val Array(trainingData, testData) = assembledDF.randomSplit(Array(0.5, 0.5), seed = 1)

// Create a logistic regression model
val lr = new LogisticRegression()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setMaxIter(10) // Set maximum number of iterations

// Fit the model on the training data
val model = lr.fit(trainingData)

// Make predictions on the testing data
val predictions = model.transform(testData)

// Print the coefficients and intercept of the model
println(s"Coefficients: ${model.coefficients}")
println(s"Intercept: ${model.intercept}")

// Display the predictions and probabilities for the testing data
predictions.select("features", "rawPrediction", "probability", "label").show(false)

// Evaluate the model performance
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("rawPrediction")
  .setMetricName("areaUnderROC")

val areaUnderROC = evaluator.evaluate(predictions)
println(s"Area under ROC curve on test data = $areaUnderROC")

// Calculate accuracy
val correctPredictions = predictions.filter("label = prediction").count().toDouble
val totalPredictions = predictions.count().toDouble
val accuracy = correctPredictions / totalPredictions

println(s"Accuracy on test data = ${accuracy * 100}%")


////

// Sample input values for testing (replace with actual input values)
val testingInputValues = Seq(
  (60, 2), // Example input values (Age, Previous_Purchases, Purchase_Amount_(USD))
  // Add more rows with different input values if needed
)

// Create a DataFrame for the testing input values
val testingInputDF = spark.createDataFrame(testingInputValues)
  .toDF("Age", "Previous_Purchases")

// Assemble features for the testing input data
val testingAssembledDF = assembler.transform(testingInputDF)

// Make predictions on the testing input data
val testingPredictions = model.transform(testingAssembledDF)

// Display the predictions for the testing input data
println("Predictions for Testing Input Values:")
testingPredictions.select("features", "rawPrediction", "probability").show(false)


// Stop the Spark session
spark.stop()