package popularioty.analytics.feedback.mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import popluarioty.analytics.feedback.ReputationConstants;
import popularioty.analytics.feedback.writable.FeedbackKey;
import popularioty.analytics.feedback.writable.FeedbackVote;
import popularioty.analytics.feedback.writable.KindOfWeight;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FeedbackMapper  extends
Mapper<LongWritable, Text, FeedbackKey, FeedbackVote>{

				
	 
	protected void map(LongWritable key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {

			 //System.out.println(value.toString());
			 ObjectMapper mapper = new ObjectMapper();
			 String s = value.toString();
			 s = s.substring(s.indexOf(",")+1,s.length());
			 
			 JsonNode feedback = mapper.readTree(s);
			//TODO filter by timestamp?
			 if(feedback.findValue("meta_feedback_id")==null)//is normal feedback
			 {
				 emmitFeedbackVoteForEntity(context, feedback);
				 emmitFeedbackVoteForDeveloper(context, feedback);
			 }
			 else{
				 //meta feedback... exactly how to handle this??
				 emmitMetaFeedbackVoteForEntity(context,feedback);
				 emmitMetaFeedbackVoteForUserGivingFeedback(context,feedback);
				 emmitMetaFeedbackVoteForDeveloper(context,feedback);				 
			 }
			 
		  }



	/**
	 * uses reputation of user giving feedback, and rating to emit key for the owner of the entity
	 * @param context
	 * @param feedback
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emmitFeedbackVoteForDeveloper(
			Context context,
			JsonNode feedback) throws IOException, InterruptedException {
		
		 String ownerId = feedback.get("entity_owner_id").asText();
		 int rating = feedback.get("rating").asInt();
		 int feedbackGiverRep = feedback.get("user_reputation").asInt();
		 FeedbackKey k = new FeedbackKey(ownerId, "user");
		 FeedbackVote vote =  new FeedbackVote(KindOfWeight.FEEDBACK_FOR_DEVELOPER,weightFeedbackRating(rating, feedbackGiverRep));
		 context.write(k, vote);		
		 
	}

	private void emmitMetaFeedbackVoteForEntity(Context context, JsonNode metafeedback)
			throws IOException, InterruptedException {

		 /*JsonNode feedback = metafeedback.get("feedback");
		 int fRating = feedback.get("rating").asInt();
		 int fFeedbackGiverRep = feedback.get("user_reputation").asInt();
		 int mRating = metafeedback.get("rating").asInt();
		 int mFeedbackGiverRep = metafeedback.get("user_reputation").asInt();
		 
		 String entityId = feedback.get("entity_id").asText();
		 String  entityType = feedback.get("entity_type").asText();
		 FeedbackKey k = new FeedbackKey(entityId, entityType);
		 FeedbackVote vote =  new FeedbackVote(KindOfWeight.METAFEEDBACK_FOR_ENTITY,weightMetaFeedbackRating(mRating,mFeedbackGiverRep,fRating,fFeedbackGiverRep));
		 */

		
	}




	private void emmitMetaFeedbackVoteForUserGivingFeedback(Context context, JsonNode metafeedback)
			throws IOException, InterruptedException {
		 
		//String x = metafeedback.get("user_id").asText();
		
	}
	
	private void emmitMetaFeedbackVoteForDeveloper(Context context, JsonNode feedback)
			throws IOException, InterruptedException {
		
	}
	/**
	 * Uses rating and reputation of user producing feedback to emmit key for the entity being rated
	 * @param context
	 * @param feedback
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emmitFeedbackVoteForEntity(Context context, JsonNode feedback)
			throws IOException, InterruptedException {
		
		 String entityId = feedback.get("entity_id").asText();
		 String  entityType = feedback.get("entity_type").asText();
		 int rating = feedback.get("rating").asInt();
		 int feedbackGiverRep = feedback.get("user_reputation").asInt();
		 FeedbackKey k = new FeedbackKey(entityId, entityType);
		 FeedbackVote vote =  new FeedbackVote(KindOfWeight.FEEDBACK_FOR_ENTITY,weightFeedbackRating(rating,feedbackGiverRep));
		 context.write(k, vote);
	}
	
	//just to avoid replicating code... ;)
	private int weightFeedbackRating(int rating, int feedbackGiverRep) 
	{
		int den = (feedbackGiverRep);
		den = (den==0?1:den);
		return rating*den;
	}
	
	
	private int weightMetaFeedbackRating(int mRating, int mFeedbackGiverRep,
			int fRating, int fFeedbackGiverRep) {
		
		int feedback = weightFeedbackRating(fRating, fFeedbackGiverRep);
		int den = (mFeedbackGiverRep);
		den = (den==0?1:den);
		return mRating*den;
		
	}
}