import os
import logging
from typing import Optional
from .utils import get_elapsed_time
from .glue_costs import gather_glue_job_data, calculate_glue_cost
from .datadog_client import build_tags, send_datadog_metrics

logger = logging.getLogger(__name__)

def report_job_cost(
    customer: str,
    environment: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    dd_api_key: Optional[str] = None,
    dd_app_key: Optional[str] = None,
    status: bool = True
) -> float:
    """
    Computes the job cost for AWS Glue or EMR and submits custom metrics to Datadog.
    
    Parameters:
      - customer: The customer identifier
      - duration_seconds: Job runtime in seconds (if not provided, uses get_elapsed_time()).
      - dd_api_key: Datadog API key (or read from DATADOG_API_KEY environment variable).
      - dd_app_key: (Optional) Datadog App key.
      - status: Job status (True for success, False for failure).
    
    Returns:
      - The calculated cost.
      
    Maintenance Considerations:
      - Regularly update the static pricing mappings and jmespath queries if the AWS Pricing API changes.
      - Monitor dependency updates for boto3, datadog, and jmespath.
      - Expand test coverage with mocks for external API calls.
      - Consider external configuration files for region mappings and pricing data in future versions.
    """
    if dd_api_key is None:
        dd_api_key = os.environ.get("DATADOG_API_KEY")
    if not dd_api_key:
        raise ValueError("Datadog API key must be provided via argument or DATADOG_API_KEY environment variable.")

    if duration_seconds is None:
        duration_seconds = get_elapsed_time()
    
    resource_data = {}
    cost = 0.0

    resource_data = gather_glue_job_data()
    cost = calculate_glue_cost(resource_data, duration_seconds)
    
    tags = build_tags(customer, resource_data, status)
    metrics = [
        {"metric": "glue.job.cost", "value": cost},
        {"metric": "glue.job.duration", "value": duration_seconds}
    ]
    
    send_datadog_metrics(metrics, tags, dd_api_key, dd_app_key)
    logger.info("Job cost: $%.4f reported to Datadog with tags: %s", cost, tags)
    return cost
