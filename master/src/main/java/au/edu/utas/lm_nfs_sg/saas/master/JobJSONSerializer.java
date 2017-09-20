package au.edu.utas.lm_nfs_sg.saas.master;

import com.google.gson.*;

import java.lang.reflect.Type;

public class JobJSONSerializer implements JsonSerializer<Job> {
	public JsonElement serialize(Job job, Type typeOfSrc, JsonSerializationContext context) {
		final JsonObject jsonObject = new JsonObject();

		jsonObject.addProperty("type", job.getJobType());
		jsonObject.addProperty("id", job.getId());
		jsonObject.addProperty("status", job.getStatus().toString());
		jsonObject.addProperty("date-started", job.getStartTimeString());

		return jsonObject;
	}
}
