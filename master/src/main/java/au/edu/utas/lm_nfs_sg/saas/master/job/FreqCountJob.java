package au.edu.utas.lm_nfs_sg.saas.master.job;

import au.edu.lm_nf_sg.saas.common.job.JobType;
import au.edu.lm_nf_sg.saas.common.worker.WorkerType;

class FreqCountJob extends Job {
	public final static String JOBTAG = "FreqCountJob";

	static {
		jobClassStringMap.put(FreqCountJob.class.toString(), "Freq Count Stream");
	}

	FreqCountJob(String i) {
		super(i);

		setWorkerType(WorkerType.PUBLIC);
		setJobType(JobType.UNBOUNDED);
	}

}
