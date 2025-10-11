import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def thanks_handler(event, context, intent_name):
    invocation_source = event.get("invocationSource", "")
    if invocation_source != "DialogCodeHook":
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Fulfilled"}
            },
            "messages": [
                {"contentType": "PlainText", "content": "You're very welcome!"}
            ]
        }
    else:
        # Don't respond in the dialog phase
        return {
            "sessionState": {
                "dialogAction": {"type": "Delegate"},
                "intent": event["sessionState"]["intent"]
            },
            "messages": []
        }
def greeting_handler(event, context, intent_name):
    invocation_source = event.get("invocationSource", "")
    if invocation_source != "DialogCodeHook":
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Fulfilled"}
            },
            "messages": [
                {"contentType": "PlainText", "content": "Hi there, how can I help you today?"}
            ]
        }
    else:
        # Don't respond in the dialog phase
        return {
            "sessionState": {
                "dialogAction": {"type": "Delegate"},
                "intent": event["sessionState"]["intent"]
            },
            "messages": []
        }

def dining_suggestion_intent_handler(event, context, intent_name):
    slots = event["sessionState"]["intent"]["slots"]
    invocation_source = event["invocationSource"]
    messages = []
    # If no user input, send initial greeting
    user_input = event.get("inputTranscript", "")
    if not user_input:
        messages.append("Hi there! I'm your personal Concierge. How can I help?")
    # Find first empty slot
    slot_to_elicit = None
    for slot_name, slot_value in slots.items():
        if not slot_value or not slot_value.get("value"):
            slot_to_elicit = slot_name
            break
    # Prepare slot prompt
    if slot_to_elicit:
        slot_prompts = {
            "date": "What date would you like the reservation for?",
            "city": "Which city?",
            "count": "How many people?",
            "cuisine": "What type of cuisine?",
            "location": "Which location?",
            "email": "What is your email address?",
            "diningTime": "At what time?"
        }
        slot_prompt = slot_prompts.get(slot_to_elicit, f"Please provide a value for {slot_to_elicit}.")
        messages.append(slot_prompt)
        lex_messages = [{"contentType": "PlainText", "content": msg} for msg in messages]
        return {
            "sessionState": {
                "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_to_elicit},
                "intent": {
                    "name": intent_name,
                    "slots": slots,
                    "state": "InProgress"
                }
            },
            "messages": lex_messages
        }
    # Fulfillment phase: send to SQS
    if invocation_source == "FulfillmentCodeHook":
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue_url = 'https://sqs.us-east-1.amazonaws.com/346225466066/Q1'
        attributes = {}
        for key, value in slots.items():
            if value and value.get("value"):
                attributes[key] = {
                    'DataType': 'String',
                    'StringValue': value['value'].get('interpretedValue', '')
                }
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="Reservation Slots",
            MessageAttributes=attributes
        )
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Fulfilled"}
            },
            "messages": [
                {"contentType": "PlainText", "content": "Thank you! Your reservation request has been sent."}
            ]
        }

    # If invoked in dialog phase but no slot to elicit, delegate back to Lex
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": event["sessionState"]["intent"]
        },
        "messages": []
    }

def lambda_handler(event, context):
    intent_name = event["sessionState"]["intent"]["name"]

    try:
        if intent_name == "DiningSuggestionIntent":
            return dining_suggestion_intent_handler(event, context, intent_name)
        elif intent_name == "GreetingIntent":
            return greeting_handler(event, context, intent_name)
        elif intent_name == "ThanksIntent":
            return thanks_handler(event, context, intent_name)
    except Exception as e:
        # Return proper Lex error format
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Failed"}
            },
            "messages": [
                {"contentType": "PlainText", "content": f"Oops! Something went wrong: {str(e)}"}
            ]
        }
