package popularioty.analytics.feedback.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popularioty.analytics.feedback.services.FeedbackCalculator;
import popularioty.analytics.feedback.start.HadoopJobs;
import popularioty.analytics.feedback.writable.FeedbackKey;
import popularioty.analytics.feedback.writable.FeedbackVote;
import popularioty.analytics.feedback.writable.KindOfWeight;
import popularioty.analytics.model.common.Utils;

import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FeedbackMapper  extends
Mapper<LongWritable, Text, FeedbackKey, FeedbackVote>{
	
	FeedbackKey feedbackKey = new FeedbackKey();
	FeedbackVote vote = new FeedbackVote();
	FeedbackCalculator calculator = new FeedbackCalculator(); 
	 
	protected void map(LongWritable key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {

			 Configuration conf = context.getConfiguration();
			 long since = conf.getLong("documents-since",HadoopJobs.DEFAULT_SINCE);
			 long until = conf.getLong("documents-until",HadoopJobs.DEFAULT_UNTIL);
		 
			 //System.out.println(value.toString());
			 ObjectMapper mapper = new ObjectMapper();
			 String s = value.toString();
			 s = s.substring(s.indexOf(",")+1,s.length());
			 s = s.trim();
			 try{
				 JsonNode feedback = mapper.readTree(s);	 
				  long date = feedback.get("date").asLong();
				
				 if(since<=date && date<until)
				 {
					 if(feedback.findValue("meta_feedback")==null)//is normal feedback
					 {
						 emmitFeedbackVoteForEntityAndDeveloper(context, feedback);
						 emmitFeedbackVoteForUserGivingFeedback(context, feedback);
					 }
					 else{
						 emmitMetaFeedbackVoteForEntity(context,feedback);
					 }
				 } 
			 }catch(Exception e){
				 Utils.log("Could not parse: ");
				 Utils.log(s);
				 e.printStackTrace();
			 }
		  }



	private void emmitFeedbackVoteForUserGivingFeedback(
			Context context,
			JsonNode feedback) throws IOException, InterruptedException {
		 
		 JsonNode s =feedback.get("used");
		 
		 if(s == null || !s.asBoolean()){ //only emmit new feedbacks
			 String uid= feedback.get("user_id").asText();
			 String feedback_id = feedback.findValue("feedback_id").asText();
			 feedbackKey.setEntityIdAndType(uid, FeedbackKey.TYPE_USER);
			 vote.setKindAndValueAndWeightAndFeedbackId( KindOfWeight.FEEDBACK_FOR_USER, 1,1,feedback_id);
			 context.write(feedbackKey, vote);
		 }
	}



	private void emmitMetaFeedbackVoteForEntity(Context context, JsonNode metafeedback)
			throws IOException, InterruptedException {

		 JsonNode s =metafeedback.get("used");
		 if(s == null || !s.asBoolean()){ //only emmit new meta_feedbacks
		
			 boolean helful = metafeedback.get("rating").asBoolean();
			 int help = (helful?1:0);
			 JsonNode feedback = metafeedback.get("feedback");
			 String feedback_id = feedback.findValue("feedback_id").asText();
			 String entityId = feedback.get("entity_id").asText();
			 String  entityType = feedback.get("entity_type").asText();			 
			 feedbackKey.setEntityIdAndType(entityId,entityType);
			 vote.setFeedbackId(feedback_id);
			 vote.setMetaFeedbackId(metafeedback.findValue("meta_feedback").asText());
			
			 Utils.log(feedbackKey.toString());
			 Utils.log("feedback id: "+feedback_id);
			 Utils.log("meta feedback id: "+metafeedback.findValue("meta_feedback").asText());
			 
			 vote.setKindAndValueAndWeightAndFeedbackId( KindOfWeight.METAFEEDBACK_FOR_ENTITY ,help,1,feedback_id);
			 context.write(feedbackKey, vote);
		 }
	}



	/**
	 * Uses rating and reputation of user producing feedba//store or emmit?   how this plays along with the array of Oks and Not Oks and the filtering of feedback??
		 ck to emmit key for the entity being rated
	 * @param context
	 * @param feedback
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emmitFeedbackVoteForEntityAndDeveloper(Context context, JsonNode feedback)
			throws IOException, InterruptedException {
		 
		 JsonNode s =feedback.get("used");
		 if(s == null || !s.asBoolean()){ //only emmit new feedbacks
			 
			 String entityId = feedback.get("entity_id").asText();
			 String  entityType = feedback.get("entity_type").asText();
			 int rating = feedback.get("rating").asInt();
			 int feedbackGiverRep = feedback.get("user_reputation").asInt();
			 String feedback_id = feedback.findValue("feedback_id").asText();
	
			 feedbackKey.setEntityIdAndType(entityId, entityType);
			 vote.setKindAndValueAndWeightAndFeedbackId(KindOfWeight.FEEDBACK_FOR_ENTITY,rating,feedbackGiverRep,feedback_id);
			 context.write(feedbackKey, vote);
		 }
	}
	
	
	
	
}