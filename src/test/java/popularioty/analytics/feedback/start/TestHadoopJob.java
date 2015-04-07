package popularioty.analytics.feedback.start;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import popularioty.analytics.feedback.mappers.FeedbackMapper;
import popularioty.analytics.feedback.mappers.FeedbackReducer;
import popularioty.analytics.feedback.writeable.FeedbackKey;
import popularioty.analytics.feedback.writeable.FeedbackVote;
import popularioty.analytics.feedback.writeable.KindOfWeight;

/**
 * Unit test for simple App.
 */
public class TestHadoopJob 
{
	MapDriver<LongWritable, Text, FeedbackKey, FeedbackVote> mapDriver;
	  ReduceDriver<FeedbackKey, FeedbackVote, Text, IntWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	 
	  @Before
	  public void setUp() {
	     FeedbackMapper mapper = new FeedbackMapper ();
	     FeedbackReducer reducer = new FeedbackReducer();
	     mapDriver = MapDriver.newMapDriver(mapper);
	     reduceDriver = ReduceDriver.newReduceDriver(reducer);
	     mapReduceDriver = MapReduceDriver.newMapReduceDriver();
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
	    
		 /*String entityId = "1427904157792f1d96255166f4593b3c19795dbe3455c";
		  String  entityType = "service_object";
		  int rating = 10;
	      FeedbackKey k = new FeedbackKey(entityId, entityType);
		  FeedbackVote vote =  new FeedbackVote(KindOfWeight.FEEDBACK_FOR_ENTITY,rating);
		  mapDriver.withInput(new LongWritable(), new Text(
	    
	        "1427904157792f1d96255166f4593b3c19795dbe3455c1428406988474,{\"user_name\":\"miserableracial\",\"text\":\"This app is a must have for any graphics engineer using OpenGL. Quick access to extension specs, easily accessible hardware limits.\",\"title\":\"Must have for any graphics engineer\",\"user_groups\":[],\"user_reputation\":8,\"user_id\":\"d5105986-030d-4d9a-bcd6-9a2f739bfc88\",\"rating\":10,\"feedback_id\":\"1427904157792f1d96255166f4593b3c19795dbe3455c1428406988474\",\"entity_id\":\"1427904157792f1d96255166f4593b3c19795dbe3455c\",\"date\":1427807745889,\"entity_type\":\"service_object\"}"));
	    mapDriver.withOutput(k, vote);
	    mapDriver.runTest();*/
	  }
	 
	  @Test
	  public void testReducer() throws IOException {
	    /*List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("6"), values);
	    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    reduceDriver.runTest();*/
	  }
	   
	  /*@Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text(
	              "655209;1;796764372490213;804422938115889;6"));
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    mapReduceDriver.runTest();
	  }*/
}
