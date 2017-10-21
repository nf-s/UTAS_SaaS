package au.edu.lm_nf_sg.saas.common.worker;

public enum WorkerType {
	// Public/private indicates what kind of workers the job can run on -
	PUBLIC,
	// private = only one job working on a worker
	PRIVATE
}