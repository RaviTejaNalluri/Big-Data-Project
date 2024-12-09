package Finding_nemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataSummarizer {

    // Summarization method using word frequency scoring
    public static Dataset<Row> summarizeText(Dataset<Row> df, String symbol, int numSentences) {
        // Broadcast the keyword for filtering
        List<String> keywords = Arrays.asList(symbol.toLowerCase());

        // UDF to calculate word frequency and summarize text
        df = df.withColumn("New_text", functions.udf((String text) -> {
            if (text == null || text.isEmpty()) return "";

            // Tokenize and calculate word frequencies
            Map<String, Long> wordCounts = Arrays.stream(text.toLowerCase().split("\\s+"))
                    .collect(Collectors.groupingBy(word -> word, Collectors.counting()));

            // Score sentences by keyword occurrence
            String[] sentences = text.split("\\. "); // Split by sentences
            List<String> sortedSentences = Arrays.stream(sentences)
                    .sorted((s1, s2) -> Long.compare(
                            keywords.stream().mapToLong(k -> wordCounts.getOrDefault(k, 0L)).sum(),
                            keywords.stream().mapToLong(k -> wordCounts.getOrDefault(k, 0L)).sum()))
                    .collect(Collectors.toList());

            // Take top N sentences
            return String.join(". ", sortedSentences.subList(0, Math.min(numSentences, sortedSentences.size())));
        }, org.apache.spark.sql.types.DataTypes.StringType).apply(functions.col("Text")));

        // Drop the old 'Text' column and return the updated DataFrame
        df = df.drop("Text");
        return df;
    }

    // Main summarization method to process files
    public static void summarizeFromCSV(String inputPath, String outputPath, SparkSession spark, int numSentences) {
        List<String> csvFiles = Arrays.asList(new java.io.File(inputPath).list((dir, name) -> name.endsWith(".csv")));

        for (String csvFile : csvFiles) {
            System.out.println("Processing file: " + csvFile);
            String filePath = inputPath + "/" + csvFile;

            // Load the CSV file
            Dataset<Row> df = spark.read().option("header", "true").csv(filePath);
            String symbol = csvFile.split("\\.")[0].toUpperCase();

            // Summarize the 'Text' column
            df = summarizeText(df, symbol, numSentences);

            // Save the updated DataFrame to the output path
            df.write().option("header", "true").csv(outputPath + "/" + symbol + ".csv");
        }
    }
}
