package popularioty.analytics.feedback.mappers;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popularioty.analytics.feedback.writeable.FeedbackKey;
import popularioty.analytics.feedback.writeable.FeedbackVote;
import popularioty.analytics.feedback.writeable.KindOfWeight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FeedbackMapper  extends
Mapper<LongWritable, Text, FeedbackKey, FeedbackVote>{

	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);
	

			
	 
	protected void map(LongWritable key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {

			 //System.out.println(value.toString());
			 ObjectMapper mapper = new ObjectMapper();
			 String s = value.toString();
			 s = s.substring(s.indexOf(",")+1,s.length());
			 
			 JsonNode feedback = mapper.readTree(s);
			 String entityId = "";
			 String entityType = "";
			 if(feedback.findValue("meta_feedback")==null)//is normal feedback
			 {
				 entityId = feedback.get("entity_id").asText();
				 entityType = feedback.get("entity_type").asText();
				 //int rating = feedback.get("rating").asInt();
				 int rating = 100;
				 FeedbackKey k = new FeedbackKey(entityId, entityType);
				 FeedbackVote vote =  new FeedbackVote(KindOfWeight.FEEDBACK_FOR_ENTITY,rating);
				 context.write(k, vote);
				 System.out.println("emmiting key "+k.toString()+vote.toString());
			 }
			 else{
				 //user providing meta feedback...
				 entityId = feedback.get("user_id").asText();
			 }
			 System.out.println(feedback.findValue("feedback_id"));
			 //status.set("6");
			 //context.write(status, addOne);
		     //655209;1;796764372490213;804422938115889;6 is the Sample record format
		     //String[] line = value.toString().split(";");
		     // If record is of SMS CDR
		     //if (Integer.parseInt(line[1]) == 1) {
		     //  status.set(line[4]);
		     //  context.write(status, addOne);
		     //}
		  }
}