import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10CountriesReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        int totalBookings = 0;

        for (IntWritable value : values) {
            totalBookings += value.get();
        }

        context.write(
                key,
                new IntWritable(totalBookings)
        );
    }
}