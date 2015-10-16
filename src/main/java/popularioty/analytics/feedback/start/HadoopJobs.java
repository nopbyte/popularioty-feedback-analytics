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

import popularioty.analytics.feedback.mappers.FeedbackMapper;
import popularioty.analytics.feedback.mappers.FeedbackReducer;
import popularioty.analytics.feedback.writable.FeedbackKey;
import popularioty.analytics.feedback.writable.FeedbackOutKey;
import popularioty.analytics.feedback.writable.FeedbackVote;

public class HadoopJobs {
	
	public static long DEFAULT_SINCE = 0;//System.currentTimeMillis()-(1000*3600*24);
	
	public static long DEFAULT_UNTIL= System.currentTimeMillis();
	
	public static void main(String[] args) throws Exception {
		

		long since = DEFAULT_SINCE;
		long until = DEFAULT_UNTIL;
		
		if(args.length>3)
		{
			since = Long.parseLong(args[2]);
			until = Long.parseLong(args[3]);
		}
		
		Configuration conf = new Configuration();
		//since one day ago...FeedbackForEntityOutput
		
		conf.setLong("documents-since", since);
		conf.setLong("documents-until", until);

		Job job = Job.getInstance(conf, "feedback");
		// This decrease the number of times the ES and CB clients have to join the ES and CB clusters respectively
		//job.setNumReduceTasks(-1);
		job.setJarByClass(HadoopJobs.class);

		job.setOutputKeyClass(FeedbackOutKey.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FeedbackMapper.class);
		job.setReducerClass(FeedbackReducer.class);

		job.setMapOutputKeyClass(FeedbackKey.class);
		job.setMapOutputValueClass(FeedbackVote.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}