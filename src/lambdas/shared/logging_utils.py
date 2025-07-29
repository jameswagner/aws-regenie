"""
Shared logging utilities for AWS Lambda functions.
Provides consistent logging configuration across all Lambda functions.
"""
import logging


def setup_lambda_logging():
    """Setup Lambda-compatible logging configuration"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers (Lambda might have some)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add a stream handler that writes to stdout (which Lambda captures)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger 