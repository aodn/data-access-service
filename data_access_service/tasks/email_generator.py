from typing import List, Tuple


def generate_started_email_subject(uuid):
    return "start processing data file whose uuid is: " + uuid

def generate_completed_email_subject(uuid):
    return "finish processing data file whose uuid is: " + uuid

def generate_started_email_content(uuid, conditions: List[Tuple[str, str]] ):
    conditionString = ""
    for condition in conditions:
        conditionString += condition[0] + ": " + condition[1] + ",\n "

    return ("already start processing" + uuid +
            ". \nThe condition(s): " +
            conditionString +
            ". \nPlease wait for the result. After the process is done, you will receive another email.")

def generate_completed_email_content(uuid, conditions:List[Tuple[str, str]], object_url):
    conditionString = ""
    for condition in conditions:
        conditionString += condition[0] + ": " + condition[1] + ", "

    return ("The result is ready. \nThe id of the dataset is: " + uuid +
            "The condition(s): " +
            conditionString +
            ". \nYou can download it. The download link is: " + object_url)
