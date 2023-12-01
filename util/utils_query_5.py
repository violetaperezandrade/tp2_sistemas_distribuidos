def get_max_values(list_frequencies):
    max_baseFare_flights = [list_frequencies[0]]
    max_value = list_frequencies[0][1]
    for i in range(1, len(list_frequencies)):
        if list_frequencies[i][1] == max_value:
            max_baseFare_flights.append(list_frequencies[i])
    return max_baseFare_flights


# def handle_query_5(flights_list):
#     price_frequency = {}
#     result = {"startingAirport": flights_list[0]["startingAirport"]}
#     for flight in flights_list:
#         baseFare = flight["baseFare"]
#         if baseFare in price_frequency:
#             price_frequency[baseFare] += 1
#         else:
#             price_frequency[baseFare] = 1
#     list_frequencies = list(price_frequency.items())
#     list_frequencies.sort(key=lambda x: x[1], reverse=True)
#     result["price_mode"] = get_max_values(list_frequencies)
#     return result

def handle_query_5(airport, base_fare_list):
    price_frequency = {}
    result = {"startingAirport": airport}
    for base_fare in base_fare_list:
        if base_fare in price_frequency:
            price_frequency[base_fare] += 1
        else:
            price_frequency[base_fare] = 1
    list_frequencies = list(price_frequency.items())
    list_frequencies.sort(key=lambda x: x[1], reverse=True)
    result["price_mode"] = get_max_values(list_frequencies)
    return result
