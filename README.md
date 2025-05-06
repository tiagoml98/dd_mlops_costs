# dd-mlops-costs

**dd-mlops-costs** is a Python library for tracking the cost of AWS Glue and EMR jobs and reporting custom metrics to Datadog. It calculates job cost based on worker/instance configuration and runtime, and sends low‑cardinality metrics to Datadog for MLOps cost monitoring.

## Features

- Separate cost calculation for AWS Glue and EMR.
- Dynamic on‑demand pricing for EMR using the AWS Pricing API with caching and retry logic, with a fallback to static pricing.
- Supports regions: `us-east-1`, `us-west-1`, and `sa-east-1`.
- Top 10 AWS Glue worker sizes supported.
- Uses the official Datadog Python library for metric submission.
- Environment detection for Glue or EMR.
- Modular design with type hints and detailed logging.
- Future enhancements planned: Dynamic pricing for Glue DPUs, Spot pricing, and external configuration files.

## Installation

Clone the repository and install with pip:

```bash
pip install .
