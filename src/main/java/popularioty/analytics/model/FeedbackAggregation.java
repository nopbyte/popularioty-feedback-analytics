package popularioty.analytics.model;

import java.util.HashMap;
import java.util.Map;

public class FeedbackAggregation {
	
	Map<String,Object> document = null;
	
	double currentValue;
	
	double totalDeltas;
	
	int countOfFeedbacks;
	
	public double getCurrentValue() {
		return currentValue;
	}
	
	public void setCurrentValue(double currentValue) {
		this.currentValue = currentValue;
	}
	
	public int getCountOfFeedbacks() {
		return countOfFeedbacks;
	}
	
	public void setCountOfFeedbacks(int countOfFeedbacks) {
		this.countOfFeedbacks = countOfFeedbacks;
	}

	public double getTotalDeltas() {
		return totalDeltas;
	}

	public void setTotalDeltas(double totalDeltas) {
		this.totalDeltas = totalDeltas;
	}

	public void setFeedbackAggregationDocument(
			Map<String, Object> aggregatedFeedback) {
		
		document = aggregatedFeedback;
		
	}
	
	public Map<String, Object>  getUpdatedDocument( String entityId, String entityType ){
		
		if(document == null)
			document = new HashMap<String, Object>();
		
		if(document.get("sub_reputation_type")==null)
			document.put("sub_reputation_type", "feedback");
		if(document.get("entity_type")==null)
			document.put("entity_type", entityType);
		if(document.get("entity_id")==null)
			document.put("entity_id", entityId);
		   
		document.put("value",currentValue);
		document.put("count",countOfFeedbacks);
		document.put("totalDeltas",totalDeltas);
		document.put("date",System.currentTimeMillis());
		return document;
	}

	public void merge(Map<String, Object> aggregatedFeedback) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
}
