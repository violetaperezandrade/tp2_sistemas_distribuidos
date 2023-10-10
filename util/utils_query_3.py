import re
RESULT_FIELDS = ["legId", "route", "stopovers", "op_code"]
DURATION_FIELD = "travelDuration"


def handle_query_3(flights):
    fastests = [{}, {}]
    for flight in flights:
        duration = convert_duration(flight[DURATION_FIELD])
        if fastests[0] == {} or duration < fastests[0].get('duration', 0):
            fastests[1] = fastests[0]
            fastests[0] = get_result(flight, duration)
        elif fastests[1] == {} or duration < fastests[1].get('duration', 0):
            fastests[1] = get_result(flight, duration)
    return fastests


def convert_duration(duration_str):
    hours_match = re.search(r'(\d+)H', duration_str)
    minutes_match = re.search(r'(\d+)M', duration_str)

    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0

    total_minutes = hours * 60 + minutes

    return total_minutes


def get_result(flight, duration):
    result = {}
    for field in RESULT_FIELDS:
        result[field] = flight[field]
    result["duration"] = duration
    return result
