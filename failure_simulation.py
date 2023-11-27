import os
from random import randint
from time import sleep

TIME_BETWEEN_DROPS = 10
REPLICATED_NODES = 3

potentially_failing_replicated_nodes = ["group_by_id", "initial_column_cleaner",
                                        "filter_by_three_stopovers",
                                        "reducer_group_by_route"]

potentially_failing_single_nodes = ["group_by_route"]

possible_failures = []
for i in range(1, REPLICATED_NODES + 1):
    for node in potentially_failing_replicated_nodes:
        possible_failures.append(f"{node}_{str(i)}")
possible_failures += potentially_failing_single_nodes


def main():
    while True:
        node_id = randint(0, len(possible_failures)-1)
        print(node_id)
        node_id = possible_failures[node_id]
        print(f"Restarting {node_id}")
        os.system(f"docker restart {node_id} -t 1")
        sleep(TIME_BETWEEN_DROPS)


if __name__ == "__main__":
    main()
