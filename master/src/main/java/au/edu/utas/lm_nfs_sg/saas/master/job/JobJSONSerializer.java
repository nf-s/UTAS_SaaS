package au.edu.utas.lm_nfs_sg.saas.master.job;

import au.edu.lm_nf_sg.saas.common.job.JobStatus;
import com.google.gson.*;

import java.lang.reflect.Type;

public class JobJSONSerializer implements JsonSerializer<Job> {
	public JsonElement serialize(Job job, Type typeOfSrc, JsonSerializationContext context) {
		final JsonObject jsonObject = new JsonObject();

		jsonObject.addProperty("type", job.getJobClassString());
		jsonObject.addProperty("id", job.getId());
		jsonObject.addProperty("status", job.getStatusString());
		jsonObject.addProperty("date-created", Job.getCalendarString(job.getCreatedDate()));

		if (job.getStatus() != JobStatus.FINISHED) {
			jsonObject.addProperty("date-completed", Job.getCalendarString(job.getEstimatedFinishDate()));
		} else {
			jsonObject.addProperty("date-completed", Job.getCalendarString(job.getFinishDate()));
		}
		jsonObject.addProperty("running-time", Job.getTimeString(job.getUsedCpuTimeInMs()));

		return jsonObject;
	}
}
