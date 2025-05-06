import os
import sys
import time
import json
import logging
import boto3
from datetime import datetime

logger = logging.getLogger(__name__)

_JOB_START_TIME = None

def start_timer() -> None:
    """Starts the job timer; call at the beginning of your job."""
    global _JOB_START_TIME
    _JOB_START_TIME = datetime.utcnow()
    logger.info("Job timer started at %s", _JOB_START_TIME)

def get_elapsed_time() -> float:
    """Returns elapsed time in seconds since start_timer() was called."""
    if _JOB_START_TIME is None:
        raise RuntimeError("Timer not started. Please call start_timer() first.")
    delta = datetime.utcnow() - _JOB_START_TIME
    return delta.total_seconds()

def get_region() -> str:
    """Detects the AWS region using the boto3 session or AWS_REGION environment variable."""
    session = boto3.Session()
    region = session.region_name or os.environ.get("AWS_REGION")
    if not region:
        raise RuntimeError("Unable to determine AWS region")
    logger.info("Detected AWS region: %s", region)
    return region

def detect_environment() -> str:
    """
    Detects whether the code is running in AWS Glue or EMR.
    Returns "glue" or "emr".
    """
    if os.path.exists('/mnt/var/lib/info/job-flow.json'):
        logger.info("Environment detected: EMR")
        return "emr"
    try:
        import awsglue.utils  # noqa: F401
        logger.info("Environment detected: Glue")
        return "glue"
    except ImportError:
        pass
    env = os.environ.get("JOB_ENVIRONMENT")
    if env:
        logger.info("Environment detected via JOB_ENVIRONMENT: %s", env.lower())
        return env.lower()
    raise RuntimeError("Unable to detect environment (not Glue and not EMR).")
