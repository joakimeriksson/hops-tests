import hopsworks
import toml, sys
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer


# Load HopsWorks Kafka configuration
conf = toml.load('config.toml')

connection = hopsworks.connection(
    host=conf['hops']['host'],
    project=conf['project']['name'],
    api_key_value=conf['project']['api_key']
)

# if an argument is given it is a job...
jobname = None
if len(sys.argv) > 0:
    jobname = sys.argv[1]

print("Connected to hopsworks")

projects = connection.get_projects()
print(projects)

project = connection.get_project(name = conf['project']['name'])

kafka_api = project.get_kafka_api()
kafka_conf = kafka_api.get_default_config()
print(kafka_conf)

jobs_api = project.get_jobs_api()
spark_config = jobs_api.get_configuration("PYSPARK")
print(spark_config)

jobs = jobs_api.get_jobs()
print("Jobs")
for job in jobs:
    exes = job.get_executions()
    last_exe = exes[-1]
    print(job.name, job.creation_time, job.creator['username'], last_exe.state, last_exe.final_status, last_exe.submission_time)

if jobname is not None:
    job = jobs_api.get_job(jobname)
    execution = job.run(await_termination=True)
    # True if job executed successfully
    print(execution.success)
    

connection.close()
