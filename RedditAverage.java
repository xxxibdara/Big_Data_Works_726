import org.json.JSONObject;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool{
    public static class RedditMapper extends Mapper<LongWritable, Text, Text, LongPairWritable>{
            private final static LongPairWritable pair = new LongPairWritable();//(0, 0)
            private Text word = new Text();

            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                    JSONObject itr = new JSONObject(value.toString());
                            word.set((String)itr.get("subreddit"));
                            pair.set(1,((Integer)itr.get("score")));//get_0() is count and get_1() is sore
                            context.write(word, pair);
            }
    }

            public static class RedditReducer
                            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
            private DoubleWritable result = new DoubleWritable();//(0,0)

            @Override
            public void reduce(Text key, Iterable<LongPairWritable> values,
                            Context context
                            ) throws IOException, InterruptedException {
                                long sum = 0;
                                long total = 0;//total scores
                                double average= 0;//average scores
                                for (LongPairWritable val : values) {
                                        sum += val.get_0();
                                        total += val.get_1();
                                }
                                average = (double) total / (double) sum;
                                result.set(average);
                                context.write(key, result);
                }
        }

        public static class RedditCombiner
                                extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
                private LongPairWritable result = new LongPairWritable();//(0,0)

                @Override
                public void reduce (Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        long sum = 0;
                        long total = 0;//total scores
                        double average= 0;//average scores
                        for (LongPairWritable val : values) {
                                sum += val.get_0();
                                total += val.get_1();
                        }
                        result.set(sum,total);
                        context.write(key, result);
                 }
        }

        public static void main(String[] args) throws Exception{
            int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
            System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
            Configuration conf = this.getConf();
            Job job = Job.getInstance(conf, "reddit average");
            job.setJarByClass(RedditAverage.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(RedditMapper.class);
            job.setCombinerClass(RedditCombiner.class);
            job.setReducerClass(RedditReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongPairWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
    }
}

        

