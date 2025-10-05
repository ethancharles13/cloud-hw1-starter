
#!/usr/bin/env python3
"""
Ingest Yelp businesses into DynamoDB.

Usage:
  python scraperv2.py --cuisine italian --location "New York, NY" --min-results 200

Env vars:
  YELP_API_KEY            - required (or pass via --yelp-api-key)
  AWS_REGION              - optional (defaults to us-east-1)
  DDB_ENDPOINT_URL        - optional (use http://localhost:8000 for DynamoDB Local)

This script will:
  - Query Yelp's Business Search API in pages (limit=50) for the given cuisine & location
  - Collect at least N businesses (default 200) or stop when results end
  - Upsert each record into DynamoDB table (default: yelp-restaurants)
  - Create the table if it doesn't exist (PK: business_id)
Stored attributes:
  business_id (PK), name, address, coordinates{lat,lon}, review_count, rating, zip_code, insertedAtTimestamp, cuisine
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
import requests

YELP_API_URL = "https://api.yelp.com/v3/businesses/search"
PAGE_LIMIT = 50   # Yelp max per request
MAX_OFFSET = 1000 # Yelp caps offset; be mindful
DEFAULT_MIN_RESULTS = 200
DEFAULT_TABLE = "yelp-restaurants"
DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_DDB_ENDPOINT = "http://localhost:8000"
from dotenv import load_dotenv
load_dotenv()

def parse_args():
    p = argparse.ArgumentParser(description="Ingest Yelp businesses into DynamoDB")
    p.add_argument("--cuisine", required=True, help="Yelp category alias (e.g., italian, chinese, indpak)")
    p.add_argument("--location", required=True, help='Location text (e.g., "New York, NY")')
    p.add_argument("--min-results", type=int, default=DEFAULT_MIN_RESULTS, help="Minimum businesses to fetch")
    p.add_argument("--table-name", default=DEFAULT_TABLE, help="DynamoDB table name")
    p.add_argument("--region", default=DEFAULT_REGION, help="AWS region (default from env or us-east-1)")
    p.add_argument("--ddb-endpoint-url", default=DEFAULT_DDB_ENDPOINT, help="DynamoDB endpoint URL (use for Local)")
    p.add_argument("--yelp-api-key",default=os.getenv("YELP_API_KEY"), help="Yelp API key (or set YELP_API_KEY)",)
    return p.parse_args()

def ensure_table(dynamodb, table_name):
    try:
        table = dynamodb.Table(table_name)
        table.load()
        return table
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    # Create table
    print(f"Creating DynamoDB table '{table_name}' ...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "business_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "business_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    print("Table created.")
    return table

def get_session(region, endpoint_url):
    if endpoint_url:
        return boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)
    return boto3.resource("dynamodb", region_name=region)

def dec(n):
    # Convert floats to DynamoDB-safe Decimals
    if n is None:
        return None
    return Decimal(str(n))

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def fetch_page(api_key, cuisine, location, limit, offset):
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "categories": cuisine,
        "location": location,
        "limit": limit,
        "offset": offset,
        # Optional: you may add "sort_by": "best_match" | "rating" | "review_count" | "distance"
    }
    r = requests.get(YELP_API_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def transform(b):
    address = ", ".join(b.get("location", {}).get("display_address") or [])
    return {
        "business_id": b.get("id"),
        "name": b.get("name"),
        "address": address,
        "coordinates": {
            "lat": dec(b.get("coordinates", {}).get("latitude")),
            "lon": dec(b.get("coordinates", {}).get("longitude")),
        },
        "review_count": b.get("review_count"),
        "rating": dec(b.get("rating")),
        "zip_code": b.get("location", {}).get("zip_code"),
        "insertedAtTimestamp": iso_now(),
        "cuisine": b.get("categories", [{}])[0].get("alias") if b.get("categories") else None,
    }

def main():
    args = parse_args()
    if not args.yelp_api_key:
        print("Missing Yelp API key. Set YELP_API_KEY or pass --yelp-api-key.", file=sys.stderr)
        sys.exit(2)

    dynamodb = get_session(args.region, args.ddb_endpoint_url)
    table = ensure_table(dynamodb, args.table_name)

    seen = set()
    collected = 0
    offset = 0
    total_from_api = None

    print(f"Fetching at least {args.min_results} '{args.cuisine}' businesses for {args.location} ...")

    with table.batch_writer(overwrite_by_pkeys=["business_id"]) as batch:
        while collected < args.min_results and offset < MAX_OFFSET:
            # If Yelp already told us the total, stop before we request beyond it
            if total_from_api is not None and offset >= min(total_from_api, MAX_OFFSET):
                print(f"Reached the end of results at offset={offset} (Yelp total={total_from_api}).")
                break

            # Compute a safe page size so we never exceed Yelp's 1000 offset cap
            page_limit = min(PAGE_LIMIT, MAX_OFFSET - offset)
            # Also trim to the reported total if we know it
            if total_from_api is not None:
                page_limit = min(page_limit, max(0, total_from_api - offset))
            if page_limit <= 0:
                print("No more results to fetch.")
                break

            try:
                data = fetch_page(args.yelp_api_key, args.cuisine, args.location, page_limit, offset)
            except requests.HTTPError as e:
                # Yelp sometimes throws 400 when offset is past the end; stop gracefully.
                if e.response is not None and e.response.status_code == 400 and offset > 0:
                    print(f"Yelp returned 400 at offset={offset}; likely no further pages. Stopping.")
                    break
                raise

            if total_from_api is None:
                total_from_api = data.get("total") or 0

            businesses = data.get("businesses", [])
            if not businesses:
                print("No more results from Yelp.")
                break

            for b in businesses:
                bid = b.get("id")
                if not bid or bid in seen:
                    continue
                item = transform(b)
                item = {k: v for k, v in item.items() if v is not None}
                if "coordinates" in item:
                    item["coordinates"] = {k: v for k, v in item["coordinates"].items() if v is not None}
                    if not item["coordinates"]:
                        del item["coordinates"]
                batch.put_item(Item=item)
                seen.add(bid)
                collected += 1

            print(f"Collected so far: {collected} (offset now {offset})")
            offset += page_limit
            time.sleep(0.6)

    print(f"Done. Collected {collected} items. Yelp reported total={total_from_api}.")
    print(f"Wrote to DynamoDB table: {args.table_name}")

if __name__ == "__main__":
    main()
