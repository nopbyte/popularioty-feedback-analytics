package popularioty.analytics.feedback.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class FeedbackOutKey implements WritableComparable<FeedbackOutKey>{
	

	
	private String entityId;
	//user, service_object, service, FEEDBACK!
	private String entityType;
	
	public FeedbackOutKey()
	{
		
	}
	public FeedbackOutKey(String entityId, String entityType)
	{
		this.entityId = entityId;
		this.entityType = entityType;
	}
	
	public void setEntityIdAndType(String entityId, String entityType)
	{
		this.entityId = entityId;
		this.entityType = entityType;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		entityId = in.readUTF();
		entityType = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(entityId);
		out.writeUTF(entityType);
	}

    
	
	@Override
	public String toString() {
		
		return entityId+"\t"+entityType+"\tfeedback";
	}
	@Override
	public int compareTo(FeedbackOutKey o) {
		return entityId.compareTo(o.entityId);/*ComparisonChain.start().compare(entityId, o.entityId)
		        .compare(entityType, o.entityType).result();*/
	}
	public String getEntityId() {
		return entityId;
	}
	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}
	public String getEntityType() {
		return entityType;
	}
	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}
	

}
