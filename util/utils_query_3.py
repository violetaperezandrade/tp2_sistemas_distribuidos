import os
import re

RESULT_FIELDS = ["legId", "route", "stopovers"]
DURATION_FIELD = "travelDuration"


def get_fastests(flight, duration, fastests, dic_changed):
    if not fastests[0] or duration < fastests[0].get('duration', 0):
        fastests[1] = fastests[0]
        fastests[0] = get_result(flight, duration)
        dic_changed = True
    elif not fastests[1] or duration < fastests[1].get('duration', 0):
        fastests[1] = get_result(flight, duration)
        dic_changed = True
    return fastests, dic_changed


def handle_query_3_register(register, dic, result_file, name,
                            old_file_filename, tmp_file_filename):
    dic_changed = False
    duration = convert_duration(register[DURATION_FIELD])
    route = register.get("route")
    route_flights = dic.get(route, [])
    if len(route_flights) < 2:
        route_flights.append(get_result(register, duration))
        dic_changed = True
        if len(route_flights) == 2:
            route_flights.sort(key=lambda x: x.get('duration', float('inf')))
            dic_changed = True
    else:
        route_flights, dic_changed = get_fastests(register, duration,
                                                  route_flights, dic_changed)
    dic[route] = route_flights

    if dic_changed:
        if not os.path.exists(result_file):
            with open(result_file, 'w+') as file:
                file.write(f"{dic}#")
        with open(tmp_file_filename, 'w+') as tmp_file:
            tmp_file.write(f"{dic}#")

        os.rename(result_file, old_file_filename)
        os.rename(tmp_file_filename, result_file)
        os.remove(old_file_filename)
    return dic


def convert_duration(duration_str):
    days_match = re.search(r'(\d+)D', duration_str)
    hours_match = re.search(r'(\d+)H', duration_str)
    minutes_match = re.search(r'(\d+)M', duration_str)

    days = int(days_match.group(1)) if days_match else 0
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0

    total_minutes = days * 24 * 60 + hours * 60 + minutes

    return total_minutes


def get_result(flight, duration):
    result = {}
    for field in RESULT_FIELDS:
        result[field] = flight[field]
    result["duration"] = duration
    return result
