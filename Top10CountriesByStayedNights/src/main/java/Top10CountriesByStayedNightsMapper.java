import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10CountriesByStayedNightsMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text countryKey = new Text();
    private final IntWritable nightsValue = new IntWritable();

    public enum StayedNightCounters {
        MALFORMED_ROWS,
        MISSING_COUNTRY,
        INVALID_NUMERIC_VALUE
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split("\\t", -1);

        // Clean TSV should contain 17 fields
        if (fields.length < 17) {
            context.getCounter(
                    StayedNightCounters.MALFORMED_ROWS
            ).increment(1);
            return;
        }

        try {
            int isCanceled =
                    Integer.parseInt(fields[1].trim());

            // Only completed / non-canceled bookings
            if (isCanceled != 0) {
                return;
            }

            int weekendNights =
                    Integer.parseInt(fields[5].trim());

            int weekNights =
                    Integer.parseInt(fields[6].trim());

            String country =
                    fields[8].trim();

            if (country.isEmpty()
                    || country.equalsIgnoreCase("UNKNOWN")
                    || country.equalsIgnoreCase("NULL")) {

                context.getCounter(
                        StayedNightCounters.MISSING_COUNTRY
                ).increment(1);

                return;
            }

            int totalNights =
                    weekendNights + weekNights;

            countryKey.set(country);
            nightsValue.set(totalNights);

            context.write(
                    countryKey,
                    nightsValue
            );

        } catch (NumberFormatException e) {

            context.getCounter(
                    StayedNightCounters.INVALID_NUMERIC_VALUE
            ).increment(1);
        }
    }
}