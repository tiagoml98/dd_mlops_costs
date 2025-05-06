import os
import json
import logging
import boto3
from typing import Dict
from .utils import get_region
from .pricing import get_emr_instance_price, get_emr_service_fee

logger = logging.getLogger(__name__)

def gather_emr_job_data() -> Dict:
    """
    Retrieves metadata for an EMR job.
    
    Reads local job-flow.json and queries the EMR API.
    Also retrieves the EMR release label if available.
    """
    data = {}
    try:
        with open('/mnt/var/lib/info/job-flow.json', 'r') as f:
            jf = json.load(f)
            data['cluster_id'] = jf.get('jobFlowId', 'unknown_cluster')
    except Exception as e:
        logger.error("Error reading EMR job-flow.json: %s", e)
        data['cluster_id'] = 'unknown_cluster'
    
    region = get_region()
    data['region'] = region
    
    try:
        emr = boto3.client("emr", region_name=region)
        resp = emr.describe_cluster(ClusterId=data['cluster_id'])
        instance_groups = resp.get("Cluster", {}).get("InstanceGroups", [])
        instances = {}
        for group in instance_groups:
            instance_type = group.get("InstanceType")
            count = group.get("RunningInstanceCount", 0)
            instances[instance_type] = instances.get(instance_type, 0) + count
        data["instances"] = instances
        data["release_label"] = resp.get("Cluster", {}).get("ReleaseLabel", "unknown")
    except Exception as e:
        logger.warning("Could not retrieve EMR cluster details via boto3: %s", e)
        data["instances"] = {}
    
    logger.info("EMR job data: %s", data)
    return data

def calculate_emr_cost(resource_data: Dict, duration_seconds: float) -> float:
    """
    Calculates the EMR job cost as:
       cost = Σ[(instance_price + emr_fee) * count * (duration_seconds / 3600)]
    """
    total_cost = 0.0
    region = resource_data.get("region")
    instances = resource_data.get("instances", {})
    for instance_type, count in instances.items():
        instance_price = get_emr_instance_price(region, instance_type)
        fee = get_emr_service_fee(region, instance_type)
        cost = (instance_price + fee) * count * (duration_seconds / 3600)
        total_cost += cost
        logger.info("EMR cost for %s: %d instances at (price: %.3f, fee: %.3f) for %d sec = %.4f",
                    instance_type, count, instance_price, fee, duration_seconds, cost)
    return total_cost
