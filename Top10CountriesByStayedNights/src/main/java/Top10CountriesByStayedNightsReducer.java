import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10CountriesByStayedNightsReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        int totalStayedNights = 0;

        for (IntWritable value : values) {
            totalStayedNights += value.get();
        }

        context.write(
                key,
                new IntWritable(totalStayedNights)
        );
    }
}