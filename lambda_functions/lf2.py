import json
import os
import logging
from typing import List, Dict

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
OS_ENDPOINT = "search-restaurantsearch-wd4lfx2rauhgd2mipjdowe73ju.aos.us-east-1.on.aws"
OS_INDEX = "restaurants"
DDB_TABLE = "yelp-restaurants"
SES_SENDER = "chz9577@nyu.edu"
RESULT_LIMIT = 3
service = "es"

# --- Clients ---
dynamodb = boto3.client("dynamodb", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)

def _open_search_client():
    session = boto3.Session()
    credentials = session.get_credentials()
    auth = AWSV4SignerAuth(credentials, REGION, service)
    return OpenSearch(
        hosts=[{"host": OS_ENDPOINT, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=10,
        max_retries=2,
        retry_on_timeout=True,
    )

os_client = _open_search_client()

def _search_top_restaurants_by_cuisine(cuisine: str, limit: int = RESULT_LIMIT) -> List[Dict]:
    """
    Returns top hits with at least fields: business_id (must be indexed in OpenSearch).
    You can tweak the query (e.g., boost rating, popularity, etc.).
    """
    query = {
        "size": limit,
        "query": {
            "match": {
                "cuisine": {
                    "query": cuisine,
                    "operator": "and"
                }
            }
        }
    }
    resp = os_client.search(index=OS_INDEX, body=query)
    hits = resp.get("hits", {}).get("hits", [])
    # Each hit: {"_id": "...", "_source": {...}}
    return hits

def _batch_get_businesses(business_ids: List[str]) -> Dict[str, Dict]:
    """
    Batch-get from DynamoDB by primary key 'business_id' (String).
    Returns whatever attributes are present in DynamoDB for each business.
    """
    if not business_ids:
        return {}

    keys = [{"business_id": {"S": bid}} for bid in business_ids]
    resp = dynamodb.batch_get_item(
        RequestItems={
            DDB_TABLE: {
                "Keys": keys
            }
        }
    )
    items = resp.get("Responses", {}).get(DDB_TABLE, [])
    
    from boto3.dynamodb.types import TypeDeserializer
    deser = TypeDeserializer()
    result = {}
    for item in items:
        py_item = {k: deser.deserialize(v) for k, v in item.items()}
        result[py_item["business_id"]] = py_item

    return result

def _format_email_html(city: str, time: str, num_people: int, cuisine: str, businesses: List[Dict]) -> str:
    lis = []
    cuisine = cuisine.capitalize()
    for b in businesses:
        lis.append(f"""
<li>
  <strong>{b.get('name','(name)')}</strong><br/>
  {b.get('address','(address)')}<br/>
  Rating: {b.get('rating','-')} | Review Count: {b.get('review_count','-')}<br/>
</li>
""")
    return f"""
<p>Hello from AWS,</p>
<p>Here are {len(businesses)} {cuisine} options for {num_people} people at <strong>{time}</strong> in {city}:</p>
<ol>
{''.join(lis)}
</ol>
<p>If you’d like me to place a reservation or refine the options, just reply here.</p>
<p>— Your Restaurant Assistant</p>
"""

def _format_email_text(city: str, time: str, num_people: int, cuisine: str, businesses: List[Dict]) -> str:
    cuisine = cuisine.capitalize()
    lines = [f"Hello,",
             f"Here are {len(businesses)} {cuisine} options for {num_people} people at {time}:"]
    for i, b in enumerate(businesses, 1):
        lines.append(f"\n{i}. {b.get('name','(name)')}")
        lines.append(f"   Address: {b.get('address','(address)')}")
        if b.get('website'):
            lines.append(f"   Website: {b['website']}")
        lines.append(f"   Rating:  {b.get('rating','-')}  Review Count: {b.get('review_count','-')}")
    lines.append("\nIf you’d like me to place a reservation or refine the options, just reply here.")
    lines.append("— Your Restaurant Assistant")
    return "\n".join(lines)

def _send_email(to_email: str, subject: str, html_body: str, text_body: str):
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": text_body, "Charset": "UTF-8"},
                "Html": {"Data": html_body, "Charset": "UTF-8"},
            },
        },
    )

def _process_one_message(msg_body: Dict):
    """
    msg_body expects: cuisine, email, city, time, num_people
    """
    cuisine = msg_body["cuisine"]
    email = msg_body["email"]
    city = msg_body["city"]
    time_str = msg_body["diningTime"]
    num_people = int(msg_body["count"])

    hits = _search_top_restaurants_by_cuisine(cuisine, RESULT_LIMIT)
    business_ids = []
    for h in hits:
        src = h.get("_source", {})
        # Prefer a dedicated field; fallback to _id if you mirrored it
        bid = src.get("business_id") or h.get("_id")
        if bid:
            business_ids.append(bid)

    ddb_items_by_id = _batch_get_businesses(business_ids)
    ordered_items = [ddb_items_by_id.get(bid, {"business_id": bid}) for bid in business_ids]
    cuisinecap = cuisine.capitalize()
    subject = f"Top {len(ordered_items)} {cuisinecap} picks for {num_people} @ {time_str}"
    html = _format_email_html(city, time_str, num_people, cuisine, ordered_items)
    text = _format_email_text(city, time_str, num_people, cuisine, ordered_items)
    _send_email(email, subject, html, text)

def _sqs_strattr(record: dict, *names: str, required: bool = True) -> str | None:
    # Prefer user message attributes
    attrs = record.get("messageAttributes") or {}
    for n in names:
        v = attrs.get(n)
        if v and (v.get("dataType") or v.get("DataType")) == "String":
            sv = v.get("stringValue") or v.get("StringValue")
            if sv:
                return sv
    # Fallback: some test payloads put user data incorrectly under `attributes`
    sysattrs = record.get("attributes") or {}
    for n in names:
        if n in sysattrs and sysattrs[n] != "":
            return sysattrs[n]
    if required:
        raise ValueError(f"Missing SQS message attribute (tried: {', '.join(names)})")
    return None

def lambda_handler(event, context):
    """
    SQS -> Lambda with a blank body and attributes:
    City (or city), count, cuisine, date, diningTime, email, location.
    Builds the pipeline message expected by _process_one_message().
    Returns partial batch response so only failed records are retried.
    """
    failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            # Read all attributes as strings
            city        = _sqs_strattr(record, "City", "city")
            count_str   = _sqs_strattr(record, "count")
            cuisine     = _sqs_strattr(record, "cuisine")
            date        = _sqs_strattr(record, "date")
            dining_time = _sqs_strattr(record, "diningTime")
            email       = _sqs_strattr(record, "email")
            location    = _sqs_strattr(record, "location")

            # Derive fields expected by your processing pipeline
            try:
                num_people = int(count_str)
            except ValueError:
                raise ValueError(f"Attribute 'count' is not an integer: {count_str!r}")

            name_guess = email.split("@", 1)[0].replace(".", " ").title()
            time_str = f"{date} {dining_time}"

            # Message for _process_one_message (keeps pass-throughs too)
            msg = {
                "cuisine": cuisine,
                "email": email,
                "name": name_guess,
                "time": time_str,
                "number_of_people": num_people,

                # pass-throughs if you want them later
                "city": city,
                "date": date,
                "diningTime": dining_time,
                "location": location,
                "count": count_str
            }

            _process_one_message(msg)
            logger.info(
                "Processed message %s for %s (%s %s, party=%s, city=%s, location=%s)",
                message_id, email, date, dining_time, num_people, city, location
            )

        except Exception as e:
            logger.exception("Failed processing message %s: %s", message_id, e)
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
