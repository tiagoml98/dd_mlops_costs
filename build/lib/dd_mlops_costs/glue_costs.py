import os
import sys
import logging
import boto3
from typing import Dict
from .utils import get_region
from .pricing import get_glue_price, GLUE_WORKER_DPUS

logger = logging.getLogger(__name__)

def gather_glue_job_data() -> Dict:
    """
    Retrieves Glue job metadata.
    
    Attempts to use awsglue.utils for parameters; falls back to environment variables.
    Also attempts to fetch details via the Glue API.
    """
    data = {}
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        data['job_name'] = args.get('JOB_NAME')
    except Exception as e:
        logger.warning("Could not retrieve Glue parameters via awsglue.utils: %s", e)
        data['job_name'] = os.environ.get("JOB_NAME", "unknown_glue_job")
    
    data['region'] = get_region()
    try:
        glue = boto3.client('glue', region_name=data['region'])
        job_details = glue.get_job(JobName=data['job_name'])
        data['worker_type'] = job_details['Job'].get('WorkerType', os.environ.get("GLUE_WORKER_TYPE", "G.1X"))
        data['number_of_workers'] = int(job_details['Job'].get('NumberOfWorkers', os.environ.get("GLUE_NUMBER_OF_WORKERS", 1)))
    except Exception as e:
        logger.warning("Unable to retrieve Glue job details via API: %s", e)
        data['worker_type'] = os.environ.get("GLUE_WORKER_TYPE", "G.1X")
        data['number_of_workers'] = int(os.environ.get("GLUE_NUMBER_OF_WORKERS", 1))
    
    logger.info("Glue job data: %s", data)
    return data

def calculate_glue_cost(resource_data: Dict, duration_seconds: float) -> float:
    """
    Calculates Glue job cost using:
       cost = (number_of_workers * DPUs per worker * (duration_seconds / 3600)) * price per DPU-hour
    """
    worker_type = resource_data.get("worker_type")
    num_workers = resource_data.get("number_of_workers", 1)
    region = resource_data.get("region")
    
    dpu_per_worker = GLUE_WORKER_DPUS.get(worker_type)
    if dpu_per_worker is None:
        raise ValueError(f"Unknown Glue worker type: {worker_type}")
    
    price = get_glue_price(region, worker_type)
    cost = num_workers * dpu_per_worker * (duration_seconds / 3600) * price
    logger.info("Calculated Glue cost: %d workers of type %s (%.2f DPUs) for %d sec at $%.3f per DPU-hour => cost $%.4f",
                num_workers, worker_type, dpu_per_worker, duration_seconds, price, cost)
    return cost
