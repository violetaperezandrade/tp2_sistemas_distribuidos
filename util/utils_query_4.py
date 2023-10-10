from functools import reduce


RESULT_FIELDS = ["legId", "route", "stopovers"]
TOTAL_FARE_FIELD = "totalFare"


def handle_query_4(flights):
    result = dict()
    avg_value = reduce(lambda a, b: a + b, map(lambda a: float(a[TOTAL_FARE_FIELD]), flights)) / len(flights)
    max_value = reduce(max, map(lambda a: float(a[TOTAL_FARE_FIELD]), flights))
    result["avg"] = avg_value
    result["max"] = max_value
    result["route"] = flights[0]["route"]
    result["op_code"] = 1
    return result