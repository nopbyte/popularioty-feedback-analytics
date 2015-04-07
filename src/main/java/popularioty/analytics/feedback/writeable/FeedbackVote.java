package popularioty.analytics.feedback.writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FeedbackVote implements Writable
{
   
	private String kindOfWeight;
	private int value;
	//in case of meta feedback this references original feedback id
	private String reference;
	
	
	
	public FeedbackVote() {
		
	}

	public FeedbackVote(String kindOfWeight, int value) {
		super();
		this.kindOfWeight = kindOfWeight;
		this.value = value;
		this.reference = "none";
	}
	
	public FeedbackVote(String kindOfWeight, int value, String reference) {
		super();
		this.kindOfWeight = kindOfWeight;
		this.value = value;
		this.reference = reference;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kindOfWeight = in.readUTF();
		value = in.readInt();
		reference = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(kindOfWeight);
		out.writeInt(value);
		out.writeUTF(reference);
	}

	public String getKindOfWeight() {
		return kindOfWeight;
	}

	public void setKindOfWeight(String kindOfWeight) {
		this.kindOfWeight = kindOfWeight;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	
}
