package popularioty.analytics.feedback.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import popluarioty.analytics.feedback.ReputationConstants;
import popularioty.analytics.feedback.services.FeedbackCalculator;
import popularioty.analytics.feedback.services.ReputationSearch;
import popularioty.analytics.feedback.writable.FeedbackKey;
import popularioty.analytics.feedback.writable.FeedbackOutKey;
import popularioty.analytics.feedback.writable.FeedbackVote;
import popularioty.analytics.feedback.writable.KindOfWeight;
import popularioty.analytics.model.FeedbackAggregation;
import popularioty.analytics.model.FeedbackData;
import popularioty.analytics.model.common.Utils;
import popularioty.commons.exception.PopulariotyException;

public class FeedbackReducer extends
Reducer<FeedbackKey, FeedbackVote, FeedbackOutKey, Text>  {

	private Text text = new Text();
	private ReputationSearch search = new ReputationSearch();
	private FeedbackCalculator calc = new FeedbackCalculator();
	/**
	 * Here we get all the feedback and meta feedback contributions sorted for the same entity with the same entity type
	 * this is THA METHOD
	 */
	protected void reduce(FeedbackKey key, Iterable<FeedbackVote> values, Context context) throws java.io.IOException, InterruptedException {
		
		if( FeedbackKey.TYPE_USER.equals(key.getEntityType()))
			processDeveloperFeedback(key,values,context);
		else
			processFeedback(key, values, context);
	    
	}
	
	
	private void processDeveloperFeedback(FeedbackKey key,
			Iterable<FeedbackVote> values,
			Context context) throws java.io.IOException, InterruptedException{
		
		int countFeedbacks = 0;
		for(FeedbackVote value: values){
			countFeedbacks  += value.getValue();
		}
			
		FeedbackOutKey outKey = new FeedbackOutKey();
		StringBuilder sb = new StringBuilder();
		outKey.setEntityId(key.getEntityId());
		outKey.setEntityType("user_giving_rating");
		sb.setLength(0);
		sb.append((Integer.toString(countFeedbacks)));
		text.set(sb.toString());
		context.write(outKey, text);
		
		
	}
	
	

	private void processFeedback(FeedbackKey key,
			Iterable<FeedbackVote> values,
			Context context) throws java.io.IOException, InterruptedException{
		double aggregation = 0;
		Map<String,FeedbackData> map = new HashMap<String,FeedbackData>();
		boolean update = false; // internal loop variable to check whether the aggregation changes. Reseted after every loop
		int direction = 0;
		
		FeedbackAggregation currentRepValue = getCurrentReputationValue(key);		
		populateFeedbackMapAndCalculatedAggregatedValue(key,values, currentRepValue.getCurrentValue(), map);
		double deltaSum = currentRepValue.getTotalDeltas();
		int count = currentRepValue.getCountOfFeedbacks();
		
		String developerId = "";
		for(String feedbackId: map.keySet()){
			FeedbackData data = getFeedbackDataFromMap(feedbackId,map);
			try {
				
				data.merge(search.getFeedbackById(feedbackId));
				developerId = data.getDeveloperId();
				double currentContribution = data.getDelta();
				if(data.shouldBeCounted() && !data.isUsed()){
					direction = 1;
					update = true;
					data.setUsed(true);					
				}
				else if(!data.shouldBeCounted() && data.isUsed()){
					direction = -1;
					update = true;
					data.setUsed(false);					
				}
				else if(data.hasNewMetaFeedbackData()){//report new meta feedback to the database 
					updateFeedbackDocument(feedbackId, data);
				}
				else{
					System.out.println("Unknown key in the reduce job: should be counted:  "+data.shouldBeCounted()+ " feedback array : ["+data.getMeta_feedback()[0]+ " , "+data.getMeta_feedback()[1]+"] feedback id:"+feedbackId+" alread used: "+ data.isUsed()+" ???????????????????????????????????????????????????????????????");
				}
				if(update){
					currentContribution = currentContribution * (float)direction;
					deltaSum += currentContribution;//increase or decrease the total
					count = count + direction; //increase or decrease the total.
					updateFeedbackDocument(feedbackId, data);
					//System.out.println("something changed: direction: "+direction+" new current contrib :"+currentContribution+" deltaSum: "+deltaSum);
					direction = 0;//just in case
					update = false;
				}				
			} catch (PopulariotyException e) {
				System.err.println("seems the feedback with id "+feedbackId+ " is not there anymore?");
			}
		}
		//should not happen that count == 0, but in some testing scenarios it can be the case.
		aggregation = (count == 0? 0: (float)((float)deltaSum/count));
		currentRepValue.setCountOfFeedbacks(count);
		currentRepValue.setTotalDeltas(deltaSum);
		currentRepValue.setCurrentValue(aggregation);
		if(aggregation != 0 ){
			
			if(developerId != null && !developerId.equals("") && !developerId.equals("unknown"))
				emmitForDeveloper(key,developerId,aggregation, count, context);			
			emmitAggregatedRepValue(key, aggregation, deltaSum, count, context);
			currentRepValue.setCountOfFeedbacks(count);
			currentRepValue.setTotalDeltas(deltaSum);
			currentRepValue.setCurrentValue(aggregation);
			storeAggregatedFeedbackValue(currentRepValue,key.getEntityId(), key.getEntityType());
		}
		
		
	}

	
	/**
	 * This method populates the map with feedback entries that reference the delta for the contribution of the feedback, 
	 * or that reference a change in the metafeedback array, or both.
	 * Also this method updates the meta feedback documents with used == true, so they can be discarded in the next passes of the feedback MR job
	 * @param key key for the entity including type and id, e.g. service_instance and id.
	 * @param values votes received by the reduce
	 * @param currentRepValue current value for the aggregation of feedback for the entity
	 * @param map reference to the map to populate
	 */
	private void populateFeedbackMapAndCalculatedAggregatedValue(FeedbackKey key, Iterable<FeedbackVote> values,
			double currentRepValue, Map<String, FeedbackData> map) {
		
		for(FeedbackVote value: values){

			if(value.getKindOfWeight().equals(KindOfWeight.FEEDBACK_FOR_ENTITY)){
				FeedbackData data = getFeedbackDataFromMap(value.getFeedbackId(),map);
				double delta = calc.weightFeedbackRating(value.getValue(), value.getWeight(), currentRepValue);
				data.setDelta(delta);				
			}
			else if(value.getKindOfWeight().equals(KindOfWeight.METAFEEDBACK_FOR_ENTITY)){
				Utils.log("meta feedback processed "+key.toString()+" feedback id: "+value.getFeedbackId()+" meta feedback id: "+value.getMetaFeedbackId());
				FeedbackData data = getFeedbackDataFromMap(value.getFeedbackId(),map);
				data.increaseMetaFeedback((int) value.getValue());//increases negative values if value is 0, or positives if the value is 1
				updateMetaFeedbackAsUsed(value.getMetaFeedbackId());
			}
		}
	}
	/**
	 * 
	 * @param feedbackId
	 * @param map
	 * @return Feedback data that is under feedbackId in the map. If it didn't exist before, this method creates it, inserts it and returns the new FeedbackData
	 *
	 */
	private FeedbackData getFeedbackDataFromMap(String feedbackId, Map<String, FeedbackData> map) {
		FeedbackData data = map.get(feedbackId);
		if(data != null)
			return  data;
		
		data = new FeedbackData();
		map.put(feedbackId, data);
		return data;
	}
	/**
	 * Gets current reputation value of the aggregation for the entity referenced by the key (service, so, etc)
	 * @param keyprivate void updateMetaFeedbackAsUsed(String metaFeedbackId) {
		try {
			 
				Map<String,Object> doc = search.getMetaFeedbackById(metaFeedbackId);
				doc.put("used", true);
				search.storeMetaFeedback(metaFeedbackId, doc);				
		} catch (PopulariotyException e) {
			System.err.println("could not update metafeedback with id : "+metaFeedbackId);
			e.printStackTrace();
		}
		
	}

	 * @return  value for the current reputation aggregation
	 */
	private FeedbackAggregation getCurrentReputationValue(FeedbackKey key) {
		
		FeedbackAggregation fa = new FeedbackAggregation();
		try {
			Map<String,Object> aggregatedFeedback = search.getSubReputationSearch(key.getEntityId(),key.getEntityType(),"feedback");
			fa.setCurrentValue(((Double)aggregatedFeedback.get("value")).doubleValue());
			fa.setTotalDeltas(((Double)aggregatedFeedback.get("totalDeltas")).doubleValue());
			fa.setCountOfFeedbacks(((Integer)(aggregatedFeedback.get("count"))).intValue());
			
			Utils.log("count of feedbacks for "+key.getEntityId()+" :"+(aggregatedFeedback.get("count")));
			
			fa.setFeedbackAggregationDocument(aggregatedFeedback);

			Utils.log("Found feedback value: "+fa.getCurrentValue()+" and an ammount of "+fa.getCountOfFeedbacks()+"feedbacks used ");
			
		} catch (Exception e) {
			
			System.out.println("feedback aggregation not found for "+key.getEntityId()+" of type "+key.getEntityType()+" maybe it is not there yet?");
			float middle = (float) ((float)ReputationConstants.MIN_REPUTATION+(((float)ReputationConstants.MAX_REPUTATION-ReputationConstants.MIN_REPUTATION)/(float)2));
			int count = 0;
			fa.setCurrentValue(middle);
			fa.setCountOfFeedbacks(count);
		}
		
		return fa;
	}
	
	
	// Methods to EMIT
	

	private void emmitForDeveloper(FeedbackKey key, String developerId, double value, int count, Context context) throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		FeedbackOutKey outKey = new FeedbackOutKey();

		outKey.setEntityId(developerId);
		outKey.setEntityType("developer");
		sb.setLength(0);
		sb.append((Double.toString(value)));
		sb.append("\t");
		sb.append(Integer.toString(count));
		text.set(sb.toString());
		context.write(outKey, text);
	}
	
	private void emmitAggregatedRepValue(FeedbackKey key,
			double aggregation, double deltaSum, int count, Context context) throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		FeedbackOutKey outKey = new FeedbackOutKey();

		outKey.setEntityId(key.getEntityId());
		outKey.setEntityType(key.getEntityType());
		sb.setLength(0);
		sb.append((Double.toString(aggregation)));
		sb.append("\t");
		sb.append((Double.toString(deltaSum)));
		sb.append("\t");
		sb.append((Integer.toString(count)));
		
		text.set(sb.toString());
		context.write(outKey, text);		
		
	}
	
	// Methods to update the documents in the database 
	
	private void updateFeedbackDocument(String feedbackId,
			FeedbackData data) {
		try {
			search.storeFeedback(feedbackId, data.getUpdatedDocument(feedbackId));
		
		} catch (PopulariotyException e) {
			System.err.println("Document with id: "+feedbackId+ " could not be updated ");
			e.printStackTrace();
		}
		
	}
	
	private void updateMetaFeedbackAsUsed(String metaFeedbackId) {
		try {
			 	System.out.println("update metafeedback with id: "+metaFeedbackId);
				Map<String,Object> doc = search.getMetaFeedbackById(metaFeedbackId);
				doc.put("used", true);
				search.storeMetaFeedback(metaFeedbackId, doc);				
		} catch (PopulariotyException e) {
			System.err.println("could not update metafeedback with id : "+metaFeedbackId);
			e.printStackTrace();
		}
		
	}

	public void storeAggregatedFeedbackValue( FeedbackAggregation aggregation, String entityId, String entityType ){
		try{
			Map doc = aggregation.getUpdatedDocument(entityId, entityType);
			search.storeSubReputationDocument(UUID.randomUUID().toString().replaceAll("-", ""), doc);
		} catch (PopulariotyException e) {
			System.err.println("could not store new feedback aggregation document ");
			e.printStackTrace();
		}
		
		
	}
	

	
}
