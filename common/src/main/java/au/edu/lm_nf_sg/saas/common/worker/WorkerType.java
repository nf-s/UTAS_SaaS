package au.edu.lm_nf_sg.saas.common.worker;

public enum WorkerType {
	// Public/private indicates what kind of workers the job can run one

	// public = many jobs can run on a single worker
	PUBLIC,
	// private = only one job working on a worker at any given time
	PRIVATE
}