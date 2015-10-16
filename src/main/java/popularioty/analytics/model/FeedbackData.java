package popularioty.analytics.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import popularioty.analytics.model.common.Utils;

public class FeedbackData{
	
	Map<String,Object> originalDocument = null;
	
	/**
	 * Meta feedback[0] is the number of times the feedback has been marked as unuseful +1, and [1] is the number of times that users have considered it to be useful+1.
	 * Start with 1 as described here: http://folk.uio.no/josang/papers/WJI2004-AAMAS.pdf
	 */
	int meta_feedback[] = {1,1};
	/**
	 * Delta affecting reputation of the entity referenced by the feedback. It is -1 at the beginning 
	 *  
	 */
	double delta = 0;
	/**
	 * tells whether this feedback was used or not.
	 */
	boolean used = false;
	
	/**
	 * If known, the developer id is stored here.
	 * 
	 */
	String developerId;
    /**
     * Flag specifying whether this object has new metafeedback (not coming from the merge function)
     */
	boolean hasNewMeta=false;
	
	public void increaseMetaFeedback(int good){
		hasNewMeta = true;
		Utils.log("increasing value for meta_feedback in position: "+good+" value before update: "+meta_feedback[good]);
		meta_feedback[good]++;
	}

	public double getDelta() {
		return delta;
	}

	public void setDelta(double delta) {
		
		this.delta = delta;
	}

	public boolean isUsed() {
		return used;
	}

	public void setUsed(boolean used) {
		this.used = used;
	}

	public int[] getMeta_feedback() {
		return meta_feedback;
	}

	public String getDeveloperId() {
		return developerId;
	}

	
	public Map<String, Object> getOriginalDocument() {
		return originalDocument;
	}

	public void setOriginalDocument(Map<String, Object> originalDocument) {
		this.originalDocument = originalDocument;
	}

	public void merge(Map<String, Object> feedback) {
		
		
		originalDocument = feedback;
		if(feedback.get("delta") != null)
			delta = ((Double)feedback.get("delta")).doubleValue();
		if(feedback.get("used") != null)
			used = ((Boolean) feedback.get("used")).booleanValue();
		if(feedback.get("entity_owner_id") != null)
			developerId = (String) feedback.get("entity_owner_id");
		if(feedback.get("meta-not-ok") != null){//if it is null the 1 for the initialization stays. Desired... afterwards when it is added with a value retrieved from a document the 1 for init is removed
			/*List l = ((List<Integer>) feedback.get("meta"));
			meta_feedback[0] += ((Integer)l.get(0)).intValue()-1; //here we must reduce the 1 of the initialization
			meta_feedback[1] += ((Integer)l.get(1)).intValue()-1; //here we must reduce the 1 of the initialization*/
			meta_feedback[0] += ((Integer)feedback.get("meta-not-ok")).intValue()-1;
			Utils.log("meta feedback  not ok found with value"+feedback.get("meta-not-ok"));
			Utils.log("new value"+meta_feedback[0]);

			
		}
		if(feedback.get("meta-ok") != null){//if it is null the 1 for the initialization stays. Desired... afterwards when it is added with a value retrieved from a document the 1 for init is removed
			meta_feedback[1] += ((Integer)feedback.get("meta-ok")).intValue()-1;
			Utils.log("meta feedback  ok found with value"+feedback.get("meta-ok"));
			Utils.log("new value"+meta_feedback[1]);
			
		}
		
	}

	public boolean shouldBeCounted() {
		
		int total = meta_feedback[0]+meta_feedback[1];
		float d = ((float)meta_feedback[0]/total);
		
		if(total>4 && d>0.5) //Expected value of next metafeedback is closer to be a "not useful" than a "useful" and this happened some times already...
			return false;
		return true;
	}
	
	public Map<String, Object>  getUpdatedDocument(String id){
		if(originalDocument == null){
			System.err.println("it seems a document dissapeared?!!!");
			originalDocument = new HashMap<String, Object>();
			originalDocument.put("feedback_id", id);
			originalDocument.put("entity_owner_id", developerId);
		}
		originalDocument.put("delta", delta);
		originalDocument.put("used", used);
		originalDocument.put("meta-not-ok", meta_feedback[0]);
		originalDocument.put("meta-ok", meta_feedback[1]);
		originalDocument.put("lastupdate",System.currentTimeMillis());
		
		return originalDocument;
	}

	public boolean hasNewMetaFeedbackData() {
		
		return hasNewMeta;
	}
	
}