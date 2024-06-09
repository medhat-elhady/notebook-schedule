import sagemaker
from sagemaker.workflow.notebook_job_step import NotebookJobStep
from sagemaker.workflow.pipeline import Pipeline
from sagemaker import session
import boto3


sagemaker_session = boto3.Session()

default_bucket = 'sagemaker-us-east-1-584910123950'
subfolder_name = "notebook-step-artifacts-pipelines/"
image_uri = "542918446943.dkr.ecr.us-east-1.amazonaws.com/sagemaker-distribution-prod:0-cpu"
kernel_name = "python3"
role = 'arn:aws:iam::584910123950:role/AmazonSageMaker-ExecutionRole' #sagemaker.get_execution_role()
notebook_artifacts = f"s3://{default_bucket}/{subfolder_name}"



pipeline_name = "nb-job-step-pipelines-demo3"
display_name = "MyNotebookSteps"
preprocess_notebook = "test_.ipynb"
preprocess_job_name = "test"
preprocess_description = "test to save file locally"
preprocess_step_name ="test"

# notebook job parameters
nb_job_params = {"default_s3_bucket": notebook_artifacts}

preprocess_nb_step = NotebookJobStep(
    name=preprocess_step_name,
    description=preprocess_description,
    notebook_job_name=preprocess_job_name,
    image_uri=image_uri,
    kernel_name=kernel_name,
    display_name=display_name,
    role=role,
    input_notebook=preprocess_notebook,
    instance_type="ml.m5.large",
    parameters=nb_job_params,
    #additional_dependencies=['requirements.txt'],
)

# create pipeline
pipeline = Pipeline(
    name=pipeline_name,
    sagemaker_session=sagemaker_session,
    steps=[preprocess_nb_step],
)


pipeline.upsert(role)
execution = pipeline.start(parameters={})
execution.wait(delay=50, max_attempts=60)
execution_steps = execution.list_steps()
print(execution_steps)