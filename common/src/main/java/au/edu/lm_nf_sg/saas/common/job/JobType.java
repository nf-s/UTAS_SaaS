package au.edu.lm_nf_sg.saas.common.job;

public enum JobType {
	// Unbounded/bounded indicates whether job has a finite running time
	// eg. a stream processing algorithm is unbounded
	UNBOUNDED,
	BOUNDED
}
