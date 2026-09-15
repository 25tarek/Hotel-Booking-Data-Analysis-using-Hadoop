import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancellationByMarketSegmentReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        int totalCancellations = 0;

        for (IntWritable value : values) {
            totalCancellations += value.get();
        }

        context.write(
                key,
                new IntWritable(totalCancellations)
        );
    }
}