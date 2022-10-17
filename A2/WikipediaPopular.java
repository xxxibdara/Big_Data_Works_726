import java.io.IOException;
import java.lang.Long;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WikipediaPopular extends Configured implements Tool {

	public static class WikipediaMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{
        private LongWritable visitedTimes = new LongWritable();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            
       String line = value.toString();//each line of the input text
			 String[] line_sep = line.split("");
			

			if (line_sep[1].equals("en")) {
				String dateAndTime = line_sep[0]; //get the data and time the page was viewed
				Text date_time = new Text(dateAndTime);// convert string to texts
				String page_name = line_sep[2];
                if( !page_name.equals("Main_Page") && !page_name.startsWith("Special:")){
					          // position 3 is the number of visited times
                    visitedTimes = new LongWritable(Long.parseLong(line_sep[3])); 
                    context.write(date_time, visitedTimes);
                }
			}
		}
	}

	public static class MaximumReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long max = 0;
      
			for(LongWritable vals: values) {
				if(max < vals.get())
					max = vals.get();
			}
			result.set(max);
			context.write(key, result);
		}
	}
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikipediaMapper.class);
        job.setCombinerClass(MaximumReducer.class);
        job.setReducerClass(MaximumReducer.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
