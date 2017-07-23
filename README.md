# UTAS_SaaS

## TO COMPILE
_COMPILE_PROJECT.bat

## TO RUN
_RUN_MASTER.bat
_RUN_LOCALWORKER.bat
_RUN_STREAMSERVER.bat

NOTE if you use RUN_STREAMSERVER.bat the default port is 1111 and the default text file is stream_server_text.txt

## TO INTERACT WITH MASTER
go to http://localhost:8080/
website can be used to create/view results/stop jobs

-- To view job and worker status --
http://localhost:8080/?action=admin

OR use commands bellow

## COMMANDS
-- To create a new job --
new <k words> <stream server hostname> <stream server port>

-- To view results for job --
results <job passcode>

-- To stop job --
stop <job passcode>

-- To view all worker hostnames/port numebrs and status --
workers_status

-- To view all job passcodes with status --
jobs_status

-- To add already started local worker --
add_worker <worker hostname> <worker port>
NOTE: if you use RUN_LOCALWORKER.bat the default port is 1234

-- To add already started cloud worker -- 
add_cloud_worker <worker hostmane> <worker port>