package au.edu.lm_nf_sg.saas.common.worker;

public enum WorkerStatus {
	INACTIVE,
	NOT_CREATED,
	CREATING,
	CREATED,
	CREATE_FAIL,
	INITIATING,
	ACTIVE,
	UNREACHABLE,
	MIGRATING,
	ERROR,
	FAILURE
}