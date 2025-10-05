from __future__ import print_function

from decimal import Decimal

from pathlib import Path
import os

import argparse
import json
import pprint
import requests
import sys
import urllib


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app

BASE_DIR = Path(__file__).resolve().parent

DB_PATH = os.path.join(BASE_DIR, 'docker/dynamodb')

# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

import os
import time
import math
import json
from typing import Dict, List, Any, Iterable

import boto3
from botocore.exceptions import ClientError
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Config
# -----------------------------
YELP_API_KEY = os.getenv("YELP_API_KEY")
if not YELP_API_KEY:
    raise SystemExit("Missing YELP_API_KEY in environment/.env")

YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
YELP_LIMIT = 50            # max per request
MIN_PER_CUISINE = 200      # target per cuisine
MAX_RESULTS_PER_QUERY = 240  # Yelp cap
REQUESTS_PER_SECOND = 0.5    # be nice to Yelp API

# Strict Manhattan bounding box (approx)
MIN_LAT, MAX_LAT = 40.7003, 40.8820
MIN_LON, MAX_LON = -74.0194, -73.9070

CUISINES = [
    ("italian", "Italian"),
    ("chinese", "Chinese"),
    ("japanese", "Japanese"),
    ("mexican", "Mexican"),
    ("indpak", "Indian"),  # Yelp categories: indian is "indpak"
]

# DynamoDB Local config
DDB_ENDPOINT = "http://localhost:8000"
DDB_REGION = "us-east-1"
TABLE_NAME = "yelp-restaurants"

DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 3



def get_business(YELP_API_KEY, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, YELP_API_KEY)

def request(host, path, YELP_API_KEY, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % YELP_API_KEY,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(YELP_API_KEY, term, location):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, YELP_API_KEY, url_params=url_params)

def query_api(term, location):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(YELP_API_KEY, term, location)

    businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    business_id = businesses[0]['id']

    print(u'{0} businesses found, querying business info ' \
        'for the top result "{1}" ...'.format(
            len(businesses), business_id))
    response = get_business(YELP_API_KEY, business_id)

    print(u'Result for business "{0}" found:'.format(business_id))
    pprint.pprint(response, indent=2)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                        type=str, help='Search term (default: %(default)s)')
    parser.add_argument('-l', '--location', dest='location',
                        default=DEFAULT_LOCATION, type=str,
                        help='Search location (default: %(default)s)')

    input_values = parser.parse_args()

    try:
        query_api(input_values.term, input_values.location)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()