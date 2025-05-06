import functools
import boto3
import json
import logging
import time
import jmespath

logger = logging.getLogger(__name__)

# --- Glue Worker Sizes: Top 10 sizes ---
GLUE_WORKER_DPUS = {
    'G.025X': 0.25,
    'G.05X': 0.5,
    'G.1X': 1,
    'G.2X': 2,
    'G.4X': 4,
    'G.8X': 8,
    'G.16X': 16,
    'G.32X': 32,
    'G.64X': 64,
    'G.128X': 128,
}

# --- Static Glue Pricing: Price per DPU-hour by region ---
GLUE_PRICE_PER_DPU_HOUR = {
    'us-east-1': 0.44,
    'us-west-1': 0.50,  # Using "us-west-1" now
    'sa-east-1': 0.428,
}

@functools.lru_cache(maxsize=32)
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

# --- EMR Pricing ---
STATIC_EMR_INSTANCE_PRICES = {
    'us-east-1': {
        'm5.xlarge': 0.192,
        'm5.2xlarge': 0.768,
    },
    'us-west-1': {
        'm5.xlarge': 0.200,
        'm5.2xlarge': 0.800,
    },
    'sa-east-1': {
        'm5.xlarge': 0.250,
        'm5.2xlarge': 1.00,
    },
}

STATIC_EMR_SERVICE_FEE = {
    'us-east-1': {
        'm5.xlarge': 0.022,
        'm5.2xlarge': 0.05,
    },
    'us-west-1': {
        'm5.xlarge': 0.025,
        'm5.2xlarge': 0.055,
    },
    'sa-east-1': {
        'm5.xlarge': 0.030,
        'm5.2xlarge': 0.060,
    },
}

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

@functools.lru_cache(maxsize=128)
@retry
def get_emr_instance_price(region: str, instance_type: str) -> float:
    """
    Retrieves the on-demand price for an EC2 instance (used for EMR)
    from the AWS Pricing API with retry logic.
    
    This implementation uses boto3 and jmespath to parse the JSON response.
    In case of failure, falls back to the static pricing mapping.
    
    The JMESPath query below is designed to extract the USD price for the hourly ("Hrs") pricing dimension,
    as On-Demand prices are typically provided per hour. Other dimensions (such as GB-Mo) are ignored.
    """
    try:
        pricing_client = boto3.client('pricing', region_name='us-east-1')  # Pricing API is in us-east-1
        region_mapping = {
            'us-east-1': 'US East (N. Virginia)',
            'us-west-1': 'US West (N. California)',
            'sa-east-1': 'South America (São Paulo)',
        }
        location = region_mapping.get(region, 'US East (N. Virginia)')
        filters = [
            {'Type': 'TERM_MATCH', 'Field': 'ServiceCode', 'Value': 'AmazonEC2'},
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
            {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
            {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
            {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
        ]
        response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters)
        price_list = response.get('PriceList', [])
        if price_list:
            price = extract_price_from_json(price_list[0])
            logger.info("Dynamic pricing for %s in %s: %s", instance_type, region, price)
            return price
        else:
            logger.warning("No dynamic pricing data found for %s in %s", instance_type, region)
            return STATIC_EMR_INSTANCE_PRICES.get(region, {}).get(instance_type, 0.0)
    except Exception as e:
        logger.error("Error fetching dynamic pricing for %s in %s: %s", instance_type, region, e)
        return STATIC_EMR_INSTANCE_PRICES.get(region, {}).get(instance_type, 0.0)

def extract_price_from_json(product_json_str: str) -> float:
    """
    Parses the AWS Pricing API JSON response to extract the on-demand hourly price.
    
    The query targets price dimensions with unit "Hrs", which represent the per-hour price,
    and extracts the price in USD. Other dimensions (e.g., GB-Mo) are ignored.
    """
    try:
        data = json.loads(product_json_str)
        # Refined query: filter for price dimensions where unit == "Hrs"
        query = "terms.OnDemand.*.priceDimensions.*[?unit=='Hrs'].pricePerUnit.USD | [0]"
        price = jmespath.search(query, data)
        if price is not None:
            return float(price)
        else:
            raise ValueError("Price with unit 'Hrs' not found in the response")
    except Exception as e:
        raise ValueError(f"Error extracting price from JSON: {e}")

def get_emr_service_fee(region: str, instance_type: str) -> float:
    """Returns the EMR service fee per instance-hour from the static mapping."""
    return STATIC_EMR_SERVICE_FEE.get(region, {}).get(instance_type, 0.0)

# Note: Spot instance pricing is not implemented in this version.
# Future enhancements may include integration of spot price retrieval.
