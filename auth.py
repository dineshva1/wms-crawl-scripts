import requests
from config import (
    RZN1_BASE_URL, RZN1_AUTH_ENDPOINT, RZN1_CLIENT_ID, RZN1_CLIENT_SECRET, RZN1_AUTHORIZATION_TOKEN
)
from logger_config import setup_logger, get_default_log_file

# Setup logger
logger = setup_logger('auth', get_default_log_file('auth'))

def get_auth_token(service='rzn1'):
    """
    Get authentication token for specified service
    Args:
        service (str): Either 'rzn1'
    Returns:
        str: Authentication token
    """
    if service == 'rzn1':
        auth_url = f"{RZN1_BASE_URL}{RZN1_AUTH_ENDPOINT}"
        client_id = RZN1_CLIENT_ID
        client_secret = RZN1_CLIENT_SECRET
        
        # Use form data for OAuth token generation
        form_data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        
        try:
            logger.info(f"Generating new auth token for {service.upper()} service...")
            logger.info(f"Using auth URL: {auth_url}")
            response = requests.post(auth_url, data=form_data)
            response.raise_for_status()
            
            token_data = response.json()
            access_token = token_data.get('access_token')
            if not access_token:
                raise ValueError(f"No access_token received in the response for {service}")
            
            logger.info(f"Successfully generated new token for {service.upper()}")
            logger.info(f"Token type: {token_data.get('token_type', 'Bearer')}")
            logger.info(f"Token expires in: {token_data.get('expires_in', 'Unknown')} seconds")
            
            # Return just the access_token without Bearer prefix as per working curl example
            return access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting auth token for {service}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
    else:
        raise ValueError("Service must be 'rzn1'")

def get_both_tokens():
    """
    Get authentication token for RZN1 service
    Returns:
        dict: Dictionary with token for RZN1 service
    """
    tokens = {}
    try:
        tokens['rzn1'] = get_auth_token('rzn1')
        return tokens
    except Exception as e:
        logger.error(f"Failed to get tokens: {str(e)}")
        raise
