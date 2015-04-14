package popularioty.analytics.feedback.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popluarioty.analytics.feedback.ReputationConstants;
import popularioty.analytics.feedback.writable.FeedbackKey;
import popularioty.analytics.feedback.writable.FeedbackVote;

public class FeedbackReducer extends
Reducer<FeedbackKey, FeedbackVote, FeedbackKey, IntWritable>  {

	private Text keyText = new Text();
	
	protected void reduce(FeedbackKey key, Iterable<FeedbackVote> values, Context context) throws java.io.IOException, InterruptedException {
	    int sum = 0;
	    int n = 0;
	    for (FeedbackVote value : values) {
	      sum += value.getValue();
	      //System.out.println("type: "+value.getKindOfWeight()+ "value "+value.getValue());
	      n++;
	    }
	    //average
	    sum /= n;
	    //normalize
	    sum /= ReputationConstants.MAX_REPUTATION;
	    keyText.set("type: "+key.getEntityType()+" id: "+key.getEntityId());
	    context.write(key, new IntWritable(sum));
	  }
	
}
