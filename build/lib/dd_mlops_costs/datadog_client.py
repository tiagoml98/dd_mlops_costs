import time
import logging
import requests

logger = logging.getLogger(__name__)

def build_tags(customer: str, environment: str, resource_data: dict, status: bool) -> list:
    """
    Builds low-cardinality tags for Datadog.

    The customer parameter is used to tag the job for cost allocation.
    """
    tags = [
        f"customer:{customer}",
        f"job_type:{environment}",
        f"status:{'success' if status else 'failed'}"
    ]
    region = resource_data.get("region", "unknown")
    tags.append(f"region:{region}")
    
    tags.append(f"glue_worker_type:{resource_data.get('worker_type', 'unknown')}")

    return tags

def send_datadog_metrics(metrics: list, tags: list, dd_api_key: str, dd_app_key: str = None) -> None:
    """
    Sends custom metrics to Datadog using the requests library.

    This function makes a POST request to the Datadog API endpoint,
    sending the given metrics along with tags. If an error occurs during
    the request, it logs the error.
    
    If dd_app_key is provided, you might use it for further operations or custom endpoints,
    although for simple metric submissions it's generally not needed.
    """
    # Construct the endpoint URL using the Datadog API key.
    url = f"https://api.datadoghq.com/api/v1/series?api_key={dd_api_key}"
    
    timestamp = int(time.time())
    series = []
    for metric in metrics:
        point = [timestamp, metric["value"]]
        series.append({
            "metric": metric["metric"],
            "points": [point],
            "tags": tags,
            "type": "gauge"
        })
    
    payload = {"series": series}
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Successfully sent metrics to Datadog: %s", response.json())
    except requests.exceptions.RequestException as e:
        logger.error("Error sending metrics to Datadog: %s", e)
