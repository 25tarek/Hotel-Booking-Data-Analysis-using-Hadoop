import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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

public class Top10CountriesDriver {

    // Mapper for Job 2:
    // Reads output of Job 1: country \t booking_count
    public static class Top10Mapper
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
                String country = parts[0].trim();
                int count = Integer.parseInt(parts[1].trim());

                context.write(
                        NullWritable.get(),
                        new Text(country + "\t" + count)
                );

            } catch (NumberFormatException e) {
                // Ignore invalid count
            }
        }
    }

    // Reducer for Job 2:
    // Keeps only the global Top 10 countries
    public static class Top10Reducer
            extends Reducer<NullWritable, Text, Text, IntWritable> {

        private static class CountryCount {
            String country;
            int count;

            CountryCount(String country, int count) {
                this.country = country;
                this.count = count;
            }
        }

        @Override
        protected void reduce(
                NullWritable key,
                Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException {

            PriorityQueue<CountryCount> top10 =
                    new PriorityQueue<>(
                            10,
                            Comparator.comparingInt(a -> a.count)
                    );

            for (Text value : values) {

                String[] parts =
                        value.toString().split("\\t");

                if (parts.length < 2) {
                    continue;
                }

                try {
                    String country = parts[0].trim();
                    int count =
                            Integer.parseInt(parts[1].trim());

                    top10.add(
                            new CountryCount(country, count)
                    );

                    if (top10.size() > 10) {
                        top10.poll();
                    }

                } catch (NumberFormatException e) {
                    // Ignore invalid rows
                }
            }

            List<CountryCount> result =
                    new ArrayList<>(top10);

            Collections.sort(
                    result,
                    new Comparator<CountryCount>() {
                        @Override
                        public int compare(
                                CountryCount a,
                                CountryCount b
                        ) {
                            return Integer.compare(
                                    b.count,
                                    a.count
                            );
                        }
                    }
            );

            for (CountryCount item : result) {
                context.write(
                        new Text(item.country),
                        new IntWritable(item.count)
                );
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: Top10CountriesDriver " +
                    "<input> <intermediate_output> <final_output>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // ==========================================
        // JOB 1: Count bookings for each country
        // ==========================================

        Job job1 = Job.getInstance(
                conf,
                "Hotel Booking - Country Counts"
        );

        job1.setJarByClass(
                Top10CountriesDriver.class
        );

        job1.setMapperClass(
                Top10CountriesMapper.class
        );

        job1.setReducerClass(
                Top10CountriesReducer.class
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

        // ==========================================
        // JOB 2: Select global Top 10 countries
        // ==========================================

        Job job2 = Job.getInstance(
                conf,
                "Hotel Booking - Top 10 Countries"
        );

        job2.setJarByClass(
                Top10CountriesDriver.class
        );

        job2.setMapperClass(
                Top10Mapper.class
        );

        job2.setReducerClass(
                Top10Reducer.class
        );

        // One reducer guarantees a GLOBAL Top 10
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