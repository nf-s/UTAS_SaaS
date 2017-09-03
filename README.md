# UTAS_SaaS

## Commands
- To create a new job
  - `new <k words> <stream server hostname> <stream server port>`

- To view results for job
  - `results <job passcode>`

- To stop job
  - `stop <job passcode>`

- To view all worker hostnames/port numbers and status
  - `workers_status`

- To view all job passcodes with status
  - `jobs_status`

- To add already started local worker
  - `add_worker <worker hostname> <worker port>`
    - NOTE: if you use RUN_LOCALWORKER.bat the default port is 1234

- To add already started cloud worker
  - `add_cloud_worker <worker hostmane> <worker port>`
