import os
import random
import sys
from random import randint
from time import sleep

TIME_BETWEEN_DROPS = 10
REPLICATED_NODES = 3

potentially_failing_replicated_nodes = ["group_by_id", "initial_column_cleaner",
                                        "filter_by_three_stopovers",
                                        "reducer_group_by_route", "query_2_column_filter",
                                        "reducer_group_by_airport", "query_5_column_filter",
                                        "distance_calculator", "filter_by_average", "avg_calculator",
                                        "group_by_id_avg", "reducer_group_by_route_q4"]

potentially_failing_single_nodes = ["group_by_route", "group_by_airport", "group_by_route_query_4"]

possible_failures = []
for i in range(1, REPLICATED_NODES + 1):
    for node in potentially_failing_replicated_nodes:
        possible_failures.append(f"{node}_{str(i)}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "healthchecker_test":
        print("Dropping all")
        possible_failures.remove("healthchecker_1")
        for node in possible_failures:
            os.system(f"docker stop {node} -t 0")
        return
    while True:
        random.shuffle(possible_failures)
        print(possible_failures)
        for node in possible_failures:
            print(f"Restarting {node}")
            os.system(f"docker restart {node} -t 0")
            sleep(TIME_BETWEEN_DROPS)


if __name__ == "__main__":
    main()
