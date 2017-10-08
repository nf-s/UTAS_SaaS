package au.edu.utas.lm_nfs_sg.saas.master;

class FreqCountJob extends Job {
	public final static String JOBTAG = "FreqCountJob";

	static {
		jobClassStringMap.put(FreqCountJob.class.toString(), "Freq Count Stream");
	}

	FreqCountJob(String i) {
		super(i);
	}

}
