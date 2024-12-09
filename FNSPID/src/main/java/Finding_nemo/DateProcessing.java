package Finding_nemo;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

public class DatePreprocessing {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Date Preprocessing and Summarization")
                .master("local[*]")
                .getOrCreate();

        String newsFolderPath = "C:\\Users\\User\\Desktop\\Big Data\\nasdaq_exteral_data.csv";
        String newsSavingPath = "C:\\Users\\User\\Desktop\\Big Data\\cleaned_data.csv";

        String stockFolderPath = "C:\\Users\\User\\Desktop\\Big Data\\full_history\\nvda.csv";
        String stockSavingPath = "C:\\Users\\User\\Desktop\\Big Data\\cleaned_history.csv";

        // Process date for news data
        dateInte(newsFolderPath, newsSavingPath, spark);

        // Process date for stock data
        dateInte(stockFolderPath, stockSavingPath, spark);

        spark.stop();
    }

    public static void dateInte(String folderPath, String savingPath, SparkSession spark) {
        List<String> csvFiles = Arrays.asList(new java.io.File(folderPath).list((dir, name) -> name.endsWith(".csv")));
        for (String csvFile : csvFiles) {
            System.out.println("Starting: " + csvFile);
            String filePath = folderPath + "/" + csvFile;

            // Load CSV file as DataFrame
            Dataset<Row> df = spark.read().option("header", "true").csv(filePath);

            // Process the date column
            df = DateProcessor.processDateColumn(df);

            // Summarize data (example summary)
            Dataset<Row> summary = DataSummarizer.summarizeData(df);
            summary.show(); // Display the summary

            // Save the processed DataFrame
            df.write().csv(savingPath + "/" + csvFile);
            System.out.println("Done: " + csvFile);
        }
    }
}

