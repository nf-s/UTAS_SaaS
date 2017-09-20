package au.edu.utas.lm_nfs_sg.saas.master;

import org.json.simple.JSONObject;

import java.io.File;

class FreqCountJob extends Job {
	public final static String JOBTAG = "FreqCountJob";

	FreqCountJob(String i) {
		super(i);
	}

}
