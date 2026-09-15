import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationByMarketSegmentMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text segmentKey = new Text();

    public enum CancellationCounters {
        MALFORMED_ROWS,
        MISSING_MARKET_SEGMENT
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
                    CancellationCounters.MALFORMED_ROWS
            ).increment(1);
            return;
        }

        String isCanceled = fields[1].trim();
        String marketSegment = fields[9].trim();

        // Only process canceled bookings
        if (!isCanceled.equals("1")) {
            return;
        }

        if (marketSegment.isEmpty()
                || marketSegment.equalsIgnoreCase("NULL")) {

            context.getCounter(
                    CancellationCounters.MISSING_MARKET_SEGMENT
            ).increment(1);

            return;
        }

        segmentKey.set(marketSegment);

        context.write(segmentKey, ONE);
    }
}