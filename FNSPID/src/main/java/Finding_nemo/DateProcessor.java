package Finding_nemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DateProcessor {

    public static Dataset<Row> processDateColumn(Dataset<Row> df) {
        df = df.withColumnRenamed("Datetime", "Date");

        // Apply the date conversion function to the 'Date' column
        df = df.withColumn("Date", functions.udf(DateUtils::convertToUTC, org.apache.spark.sql.types.DataTypes.StringType)
                .apply(functions.col("Date")));

        // Convert 'Date' to timestamp and sort in descending order
        df = df.withColumn("Date", functions.to_timestamp(functions.col("Date"), "yyyy-MM-dd HH:mm:ss 'UTC'"));
        df = df.orderBy(functions.col("Date").desc());

        return df;
    }
}
