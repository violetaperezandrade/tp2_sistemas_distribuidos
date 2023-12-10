import os
import random
import sys
from time import sleep

TIME_BETWEEN_DROPS = 5
REPLICATED_NODES = 3


def generate_node_list(with_healthcheckers=False):

    potentially_failing_replicated_nodes = ["filter_by_average"]

    potentially_failing_single_nodes = ["group_by_route_query_4"]

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
            while "healthchecker_3" in possible_failures:
                possible_failures.remove("healthchecker_3")
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
            print(f"Stopping {node}")
            os.system(f"docker stop {node} -t 0")
            sleep(TIME_BETWEEN_DROPS)


if __name__ == "__main__":
    main()
