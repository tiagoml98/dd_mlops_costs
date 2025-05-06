# dd-mlops-costs

**dd-mlops-costs** is a Python library for tracking the cost of AWS Glue jobs and reporting custom metrics to Datadog. It calculates job cost based on worker/instance configuration and runtime, and sends low‑cardinality metrics to Datadog for MLOps cost monitoring.

## Features

- Cost calculation for AWS Glue
- Supports regions: `us-east-1`, `us-west-1`, and `sa-east-1`.
- Top AWS Glue worker sizes supported.
- Uses the Datadog API for metric submission.
- Modular design with type hints and detailed logging.
- Future enhancements planned: Dynamic pricing for Glue DPUs and external configuration files.

## Installation

Download the .whl file and place into an S3 Bucket. Reference the S3 bucket location in the Advanced Properties of the Glue Job

## Usage

Utilize the following functions, at the beginning and end of the PySpark script:
````
# Example PySpark Script

# Import
from dd_mlops_costs import report_job_cost, start_timer

# Beginning of Script
start_timer()

# Body of PySpark script

# End of Script
report_job_cost(customer="customer-name", dd_api_key="xxxxx", dd_app_key="xxxxx")



````