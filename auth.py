import requests
from config import (
    RZN1_BASE_URL, RZN1_AUTH_ENDPOINT, RZN1_CLIENT_ID, RZN1_CLIENT_SECRET
)
from logger_config import setup_logger, get_default_log_file

# Setup logger
logger = setup_logger('auth', get_default_log_file('auth'))

def get_auth_token(service='rzn1'):
    """
    Get authentication token for specified service
    Args:
        service (str): Either 'rzn1' or 'rzn'
    Returns:
        str: Authentication token
    """
    if service == 'rzn1':
        auth_url = f"{RZN1_BASE_URL}{RZN1_AUTH_ENDPOINT}"
        client_id = RZN1_CLIENT_ID
        client_secret = RZN1_CLIENT_SECRET
    # elif service == 'rzn':
    #     auth_url = f"{RZN_BASE_URL}{RZN_AUTH_ENDPOINT}"
    #     client_id = RZN_CLIENT_ID
    #     client_secret = RZN_CLIENT_SECRET
    else:
        raise ValueError("Service must be either 'rzn1'")
    
    # Use form data as shown in the curl example
    form_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    
    try:
        logger.info(f"Getting auth token for {service.upper()} service...")
        logger.info(f"Using auth URL: {auth_url}")
        # Use data parameter for form data instead of json
        response = requests.post(auth_url, data=form_data)
        response.raise_for_status()
        
        token = response.json().get('access_token')
        if not token:
            raise ValueError(f"No token received in the response for {service}")
        
        logger.info(f"Successfully obtained token for {service.upper()}")
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting auth token for {service}: {str(e)}")
        raise

def get_both_tokens():
    """
    Get authentication token for RZN1 service
    Returns:
        dict: Dictionary with token for RZN1 service
    """
    tokens = {}
    try:
        tokens['rzn1'] = get_auth_token('rzn1')
        # tokens['rzn'] = get_auth_token('rzn')
        return tokens
    except Exception as e:
        logger.error(f"Failed to get tokens: {str(e)}")
        raise
