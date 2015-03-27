package popularioty.analytics.feedback.start;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import popularioty.analytics.feedback.mappers.GenericEntityMapper;
import popularioty.analytics.feedback.mappers.GenericEntityReducer;

public class HadoopJobs {
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(HadoopJobs.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(GenericEntityMapper.class);
		job.setReducerClass(GenericEntityReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}