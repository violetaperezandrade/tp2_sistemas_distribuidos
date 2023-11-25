import re

RESULT_FIELDS = ["legId", "route", "stopovers"]
DURATION_FIELD = "travelDuration"


def get_fastests(flight, duration, fastests):
    if not fastests[0] or duration < fastests[0].get('duration', 0):
        fastests[1] = fastests[0]
        fastests[0] = get_result(flight, duration)
    elif not fastests[1] or duration < fastests[1].get('duration', 0):
        fastests[1] = get_result(flight, duration)
    return fastests


def handle_query_3_register(register, dic, result_file):
    duration = convert_duration(register[DURATION_FIELD])
    route = register.get("route")
    client_id = register.get("client_id")
    route_flights = dic.get(route, [])
    if len(route_flights) < 2:
        route_flights.append(get_result(register, duration))
        if len(route_flights) == 2:
            route_flights.sort(key=lambda x: x.get('duration', float('inf')))
    else:
        route_flights = get_fastests(register, duration, route_flights)
    dic[route] = route_flights
    dic["client_id"] = client_id
    # Hacer que esto solo pase si hay una update efectiva
    # Hacer que tolere fallas durante escritura (tener copia de seguridad en 3,
    # 4 y 5ta linea)
    with open(result_file, "r+") as file:
        lines = file.readlines()
        lines[int(client_id)-1] = str(dic) + '\n'
        file.seek(0)
        file.writelines(lines)
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
