import os
import random
import sys
from time import sleep

TIME_BETWEEN_DROPS = 11
REPLICATED_NODES = 3


def generate_node_list(with_healthcheckers=False):
    #     potentially_failing_replicated_nodes = ["group_by_id",
    #                                             "initial_column_cleaner",
    #                                             "filter_by_three_stopovers",
    #                                             "reducer_group_by_route",
    #                                             "query_2_column_filter",
    #                                             "reducer_group_by_airport",
    #                                             "query_5_column_filter",
    #                                             "distance_calculator",
    #                                             "filter_by_average",
    #                                             "avg_calculator",
    #                                             "group_by_id_avg",
    #                                             "reducer_group_by_route_q4"]

    #     potentially_failing_single_nodes = ["group_by_route",
    #                                         "group_by_airport",
    #                                         "group_by_route_query_4"]
    potentially_failing_replicated_nodes = ["group_by_id",
                                            "initial_column_cleaner",
                                            "filter_by_three_stopovers",
                                            "healthchecker"]

    potentially_failing_single_nodes = ["query_handler"]

    possible_failures = []
    for i in range(1, REPLICATED_NODES + 1):
        for node in potentially_failing_replicated_nodes:
            possible_failures.append(f"{node}_{str(i)}")
        if with_healthcheckers:
            possible_failures.append(f"healthchecker_{str(i)}")
    possible_failures += potentially_failing_single_nodes
    return possible_failures


def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == "-all":
            possible_failures = generate_node_list(True)
            print("Dropping all")
            possible_failures.remove("healthchecker_1")
            for node in possible_failures:
                os.system(f"docker stop {node} -t 0")
            return
        elif sys.argv[1] == "-server":
            print("Dropping server for file deletion verification")
            os.system("docker stop server -t 0")
            sleep(5)
            os.system("docker start server")
        else:
            print("Invalid flag")
            return
    possible_failures = generate_node_list()
    while True:
        random.shuffle(possible_failures)
        for node in possible_failures:
            print(f"Stoping {node}")
            os.system(f"docker stop {node} -t 0")
            sleep(TIME_BETWEEN_DROPS)


if __name__ == "__main__":
    main()
