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

public class Top10CountriesByStayedNightsDriver {

    // Job 2 Mapper:
    // Reads Job 1 output: country \t total_stayed_nights
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
                int totalNights =
                        Integer.parseInt(parts[1].trim());

                context.write(
                        NullWritable.get(),
                        new Text(country + "\t" + totalNights)
                );

            } catch (NumberFormatException e) {
                // Ignore invalid rows
            }
        }
    }

    // Job 2 Reducer:
    // Keeps the global Top 10 countries
    public static class Top10Reducer
            extends Reducer<NullWritable, Text, Text, IntWritable> {

        private static class CountryNights {
            String country;
            int nights;

            CountryNights(String country, int nights) {
                this.country = country;
                this.nights = nights;
            }
        }

        @Override
        protected void reduce(
                NullWritable key,
                Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException {

            PriorityQueue<CountryNights> top10 =
                    new PriorityQueue<>(
                            10,
                            Comparator.comparingInt(a -> a.nights)
                    );

            for (Text value : values) {

                String[] parts =
                        value.toString().split("\\t");

                if (parts.length < 2) {
                    continue;
                }

                try {
                    String country = parts[0].trim();

                    int totalNights =
                            Integer.parseInt(parts[1].trim());

                    top10.add(
                            new CountryNights(
                                    country,
                                    totalNights
                            )
                    );

                    if (top10.size() > 10) {
                        top10.poll();
                    }

                } catch (NumberFormatException e) {
                    // Ignore invalid rows
                }
            }

            List<CountryNights> result =
                    new ArrayList<>(top10);

            Collections.sort(
                    result,
                    new Comparator<CountryNights>() {
                        @Override
                        public int compare(
                                CountryNights a,
                                CountryNights b
                        ) {
                            return Integer.compare(
                                    b.nights,
                                    a.nights
                            );
                        }
                    }
            );

            for (CountryNights item : result) {
                context.write(
                        new Text(item.country),
                        new IntWritable(item.nights)
                );
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: Top10CountriesByStayedNightsDriver " +
                    "<input> <intermediate_output> <final_output>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // ==========================================
        // JOB 1: Sum stayed nights for each country
        // ==========================================

        Job job1 = Job.getInstance(
                conf,
                "Hotel Booking - Total Stayed Nights by Country"
        );

        job1.setJarByClass(
                Top10CountriesByStayedNightsDriver.class
        );

        job1.setMapperClass(
                Top10CountriesByStayedNightsMapper.class
        );

        job1.setReducerClass(
                Top10CountriesByStayedNightsReducer.class
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
                "Hotel Booking - Top 10 Countries by Stayed Nights"
        );

        job2.setJarByClass(
                Top10CountriesByStayedNightsDriver.class
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