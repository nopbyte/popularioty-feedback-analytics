package popularioty.analytics.feedback.services;

import popluarioty.analytics.feedback.ReputationConstants;

public class FeedbackCalculator {
	
	
	
	
	/**
	 * This method gives the result of the weighing between the feedback and the reputation of the user providing it. 
	 * @param rating
	 * @param feedbackGiverRep
	 * @return rating is returned only when the user has the best reputation possible, otherwise the feedback weight moves closer to the middle of the reputation scale depending on the user's reputation.
	 * i.e. Users with worst reputation giving feedback, will result in a feedback value close to the middle of the scale. 
	 */
	public double weightFeedbackRating(float rating, float feedbackGiverRep, double current) 
	{
		double returnValue = current ;
		returnValue +=  (((rating-current)*feedbackGiverRep)/ReputationConstants.MAX_REPUTATION);
		return returnValue;
		
	}
}
