
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def greetLexbot(user_message, sessionId):
    try:
        lex_client = boto3.client("lexv2-runtime")
        response = lex_client.recognize_text(
            botId="MWUOKH6H8A",              
            botAliasId="RBDTHRHNVZ",       
            localeId="en_US",
            sessionId=sessionId,
            text=user_message)
        messages = response.get("messages", [])
        formatted_messages = []

        for msg in messages:
            if "unstructured" in msg:
                text = msg["unstructured"].get("text", "")
            else:
                text = msg.get("content", "")
            # Add each message in the format the frontend expects
            formatted_messages.append({
                "type": "unstructured",
                "unstructured": {"text": text}
            })

        if not formatted_messages:
            formatted_messages.append({
                "type": "unstructured",
                "unstructured": {"text": "No response from Lex."}
            })
        return formatted_messages
    except Exception as e:
        logger.error("Lex call failed: %s", str(e))
        return [{
            "type": "unstructured",
            "unstructured": {"text": f"Sorry, I encountered an error. Please try again. ({str(e)})"}
        }]

def lambda_handler(event, context):
    # get initial message from customer
    body = json.loads(event.get("body", "{}"))

    #try to get message form the website structure
    try:
        user_message = body["messages"][0]["unstructured"]["text"]
    except (KeyError, IndexError):
        # fallback
        user_message = body.get("message", "Hello")
    #hard coded sessionId because front end was not set up correctly to store it
    sessionId = "abc123"
    response_messages = greetLexbot(user_message, sessionId)
    logger.info("Full Lex response: %s", json.dumps(response_messages, indent=2))
    # Return to frontend
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*"
        },
        "body": json.dumps({
            "messages": response_messages
        })
    }
