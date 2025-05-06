import time
import logging
from datadog import initialize, api

logger = logging.getLogger(__name__)

def build_tags(cnpj: str, environment: str, resource_data: dict, status: bool) -> list:
    """
    Builds low-cardinality tags for Datadog.
    
    The cnpj parameter (currently a temporary identifier) is used to tag the job for cost allocation.
    In the future, this may be mapped to a customer name.
    """
    tags = [
        f"customer:{cnpj}",
        f"job_type:{environment}",
        f"status:{'success' if status else 'failed'}"
    ]
    region = resource_data.get("region", "unknown")
    tags.append(f"region:{region}")
    
    if environment == "glue":
        tags.append(f"glue_worker_type:{resource_data.get('worker_type', 'unknown')}")
    elif environment == "emr":
        instances = resource_data.get("instances", {})
        instance_types = sorted(instances.keys())
        if instance_types:
            tags.append("emr_instance_types:" + ",".join(instance_types))
        if resource_data.get("release_label"):
            tags.append(f"emr_release_label:{resource_data['release_label']}")
    return tags

def send_datadog_metrics(metrics: list, tags: list, dd_api_key: str, dd_app_key: str = None) -> None:
    """
    Sends custom metrics to Datadog using the official Datadog Python library.
    """
    options = {'api_key': dd_api_key}
    if dd_app_key:
        options['app_key'] = dd_app_key
    initialize(**options)
    
    timestamp = int(time.time())
    for metric in metrics:
        point = (timestamp, metric["value"])
        try:
            api.Metric.send(metric=metric["metric"], points=[point], tags=tags, type="gauge")
            logger.info("Sent metric: %s", metric["metric"])
        except Exception as e:
            logger.error("Error sending metric %s: %s", metric["metric"], e)
