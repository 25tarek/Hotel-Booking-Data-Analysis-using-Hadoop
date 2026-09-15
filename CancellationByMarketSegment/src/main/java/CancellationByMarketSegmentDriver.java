import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancellationByMarketSegmentDriver {

    // Job 2 Mapper:
    // Reads Job 1 output: market_segment \t cancellation_count
    public static class SortMapper
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\\t");

            if (parts.length < 2) {
                return;
            }

            try {
                String marketSegment = parts[0].trim();
                int count = Integer.parseInt(parts[1].trim());

                context.write(
                        NullWritable.get(),
                        new Text(marketSegment + "\t" + count)
                );

            } catch (NumberFormatException e) {
                // Ignore invalid rows
            }
        }
    }

    // Job 2 Reducer:
    // Sorts market segments by cancellation count descending
    public static class SortReducer
            extends Reducer<NullWritable, Text, Text, IntWritable> {

        private static class SegmentCount {
            String segment;
            int count;

            SegmentCount(String segment, int count) {
                this.segment = segment;
                this.count = count;
            }
        }

        @Override
        protected void reduce(
                NullWritable key,
                Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException {

            List<SegmentCount> results = new ArrayList<>();

            for (Text value : values) {

                String[] parts =
                        value.toString().split("\\t");

                if (parts.length < 2) {
                    continue;
                }

                try {
                    String segment = parts[0].trim();
                    int count =
                            Integer.parseInt(parts[1].trim());

                    results.add(
                            new SegmentCount(segment, count)
                    );

                } catch (NumberFormatException e) {
                    // Ignore invalid rows
                }
            }

            Collections.sort(
                    results,
                    new Comparator<SegmentCount>() {
                        @Override
                        public int compare(
                                SegmentCount a,
                                SegmentCount b
                        ) {
                            return Integer.compare(
                                    b.count,
                                    a.count
                            );
                        }
                    }
            );

            for (SegmentCount item : results) {
                context.write(
                        new Text(item.segment),
                        new IntWritable(item.count)
                );
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: CancellationByMarketSegmentDriver " +
                    "<input> <intermediate_output> <final_output>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // ======================================
        // JOB 1: Count cancellations by segment
        // ======================================

        Job job1 = Job.getInstance(
                conf,
                "Hotel Booking - Cancellations by Market Segment"
        );

        job1.setJarByClass(
                CancellationByMarketSegmentDriver.class
        );

        job1.setMapperClass(
                CancellationByMarketSegmentMapper.class
        );

        job1.setReducerClass(
                CancellationByMarketSegmentReducer.class
        );

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job1,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job1,
                new Path(args[1])
        );

        boolean job1Success =
                job1.waitForCompletion(true);

        if (!job1Success) {
            System.exit(1);
        }

        // ======================================
        // JOB 2: Sort counts descending
        // ======================================

        Job job2 = Job.getInstance(
                conf,
                "Hotel Booking - Sorted Cancellations by Market Segment"
        );

        job2.setJarByClass(
                CancellationByMarketSegmentDriver.class
        );

        job2.setMapperClass(
                SortMapper.class
        );

        job2.setReducerClass(
                SortReducer.class
        );

        // One reducer ensures one globally sorted result
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(
                NullWritable.class
        );

        job2.setMapOutputValueClass(
                Text.class
        );

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job2,
                new Path(args[1])
        );

        FileOutputFormat.setOutputPath(
                job2,
                new Path(args[2])
        );

        boolean job2Success =
                job2.waitForCompletion(true);

        System.exit(job2Success ? 0 : 1);
    }
}