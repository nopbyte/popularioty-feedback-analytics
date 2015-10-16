package popularioty.analytics.feedback.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FeedbackVote implements Writable
{
   
	/**
	 * Kind of feedback is taken from interface KindOfWeight in writable package to show what is being sent.
	 */
	private String kindOfWeight;
	/**
	 * value for the feedback, i.e. the rating itself
	 */
	private float value;
	/**
	 * weight factor, this will be the user reputation when feedback for an entity is being processed, otherwise it is no really used
	 */
	private float weight;

	/**
	 * The feedbackid. 
	 */
	private String feedbackId="";
	
	/**
	 * Only used when the meta feedback is being sent.
	 */
	private String metaFeedbackId="";
	
	public FeedbackVote() {
		
	}
	
	public void setKindAndValueAndWeightAndFeedbackId(String kindOfWeight, float value, float weight,String feedbackId) {
		this.kindOfWeight = kindOfWeight;
		this.value = value;
		this.weight =weight;
		this.feedbackId = feedbackId;
	}
	
	public FeedbackVote(String kindOfWeight, float value) {
		super();
		this.kindOfWeight = kindOfWeight;
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kindOfWeight = in.readUTF();
		value = in.readFloat();
		weight = in.readFloat();
		feedbackId = in.readUTF();
		metaFeedbackId = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(kindOfWeight);
		out.writeFloat(value);
		out.writeFloat(weight);
		out.writeUTF(feedbackId);
		out.writeUTF(metaFeedbackId);
	}

	public String getKindOfWeight() {
		return kindOfWeight;
	}

	public void setKindOfWeight(String kindOfWeight) {
		this.kindOfWeight = kindOfWeight;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	
	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public String getFeedbackId() {
		return feedbackId;
	}

	public void setFeedbackId(String feedbackId) {
		this.feedbackId = feedbackId;
	}

	public String getMetaFeedbackId() {
		return metaFeedbackId;
	}

	public void setMetaFeedbackId(String metaFeedbackId) {
		this.metaFeedbackId = metaFeedbackId;
	}
	
	
}
