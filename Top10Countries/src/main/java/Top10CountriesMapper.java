import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10CountriesMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text countryKey = new Text();

    public enum BookingCounters {
        MALFORMED_ROWS,
        MISSING_COUNTRY
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();

        // Keep empty fields while splitting the TSV row
        String[] fields = line.split("\\t", -1);

        // Clean dataset should contain 17 fields
        if (fields.length < 17) {
            context.getCounter(
                    BookingCounters.MALFORMED_ROWS
            ).increment(1);
            return;
        }

        // Country is column/index 8 in the cleaned TSV
        String country = fields[8].trim();

        if (country.isEmpty()
                || country.equalsIgnoreCase("UNKNOWN")) {

            context.getCounter(
                    BookingCounters.MISSING_COUNTRY
            ).increment(1);

            return;
        }

        countryKey.set(country);

        // Each valid booking contributes 1 to its country
        context.write(countryKey, ONE);
    }
}