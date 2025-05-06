import functools
import boto3
import json
import logging
import time
import jmespath

logger = logging.getLogger(__name__)

# --- Glue Worker Sizes: Top sizes ---
GLUE_WORKER_DPUS = {
    'G.025X': 0.25,
    'G.1X': 1,
    'G.2X': 2,
    'G.4X': 4,
    'G.8X': 8
}

# --- Static Glue Pricing: Price per DPU-hour by region ---
GLUE_PRICE_PER_DPU_HOUR = {
    'us-east-1': 0.44,
    'us-west-1': 0.44, 
    'sa-east-1': 0.69,
}

def get_glue_price(region: str, worker_type: str) -> float:
    """
    Returns the price per DPU-hour for the given region.
    Currently uses a static mapping as dynamic pricing for Glue DPUs is not reliably available.
    """
    price = GLUE_PRICE_PER_DPU_HOUR.get(region)
    if price is None:
        logger.warning("No Glue pricing data for region %s; defaulting to 0.44", region)
        price = 0.44
    return price

def retry(func):
    """
    A simple retry decorator with exponential backoff.
    Retries up to 3 times.
    
    Note: For production use, consider configuring boto3's built-in retry settings
    via botocore.config.Config. See: 
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#botocore-config
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        retries = 3
        delay = 1  # initial delay in seconds
        for i in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if i < retries - 1:
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise e
    return wrapper