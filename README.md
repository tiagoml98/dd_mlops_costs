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

Clone the repository and install with pip:

```bash
pip install .
