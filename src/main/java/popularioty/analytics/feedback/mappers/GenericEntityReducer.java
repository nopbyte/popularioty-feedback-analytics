package popularioty.analytics.feedback.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenericEntityReducer extends
Reducer<Text, IntWritable, Text, IntWritable>  {

	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable value : values) {
	      sum += value.get();
	    }
	    context.write(key, new IntWritable(sum));
	  }
	
}
