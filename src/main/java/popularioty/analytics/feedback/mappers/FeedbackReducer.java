package popularioty.analytics.feedback.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popularioty.analytics.feedback.writeable.FeedbackKey;
import popularioty.analytics.feedback.writeable.FeedbackVote;

public class FeedbackReducer extends
Reducer<FeedbackKey, FeedbackVote, Text, IntWritable>  {

	private Text keyText = new Text();
	
	protected void reduce(FeedbackKey key, Iterable<FeedbackVote> values, Context context) throws java.io.IOException, InterruptedException {
	    int sum = 0;
	    for (FeedbackVote value : values) {
	      sum += value.getValue();
	      System.out.println("type: "+value.getKindOfWeight()+ "value "+value.getValue());
	    }
	    keyText.set(key.getEntityId());
	    context.write(keyText, new IntWritable(sum));
	  }
	
}
