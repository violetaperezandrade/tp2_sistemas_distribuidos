import sys
import yaml

nodes_with_dependencies = ["initial_column_cleaner_1", "query_2_column_filter_1",
                           "group_by_route", "group_by_airport",
                           "avg_calculator_1"]

nodes_to_scale = ["initial_column_cleaner_1", "filter_by_three_stopovers_1",
                  "query_2_column_filter_1", "avg_calculator_1", "distance_calculator_1",
                  "query_5_column_filter_1", "filter_by_average_1"]

reducers_to_scale = ["reducer_group_by_route_1", "reducer_group_by_airport_1",
                     "reducer_group_by_route_q4_1"]


def add_depends(list_dependencies, node_scaler, reducer_scaler):
    new_dependencies = []
    for dependency in list_dependencies:
        base = dependency[:-1]
        if dependency in reducers_to_scale:
            for i in range(3, reducer_scaler + 1):
                new_dependency = base + str(i)
                new_dependencies.append(new_dependency)
            return
        for i in range(2, node_scaler + 1):
            new_dependency = base + str(i)
            new_dependencies.append(new_dependency)
    list_dependencies += new_dependencies


def multiply_node(node, multiplier, base):
    for i in range(2, multiplier + 1):
        index = base[:-1] + str(i)
        node[index] = node[base].copy()
        node[index]["container_name"] = index
        if base[:-1] == "filter_by_average_":
            node[index]["environment"] = node[base]["environment"].copy()
            node[index]["environment"][5] = f"ID={i}"
            continue


def multiply_reducers(node, reducer_scaler, base):
    for i in range(3, reducer_scaler + 1):
        index = base[:-1] + str(i)
        node[index] = node[base].copy()
        node[index]["container_name"] = index


def main():
    if len(sys.argv) <= 2:
        print("Not enough arguments")
        return
    node_scaler = int(sys.argv[1])
    reducer_scaler = int(sys.argv[2])
    if node_scaler < 2:
        print("Node scaler must be at least 2")
        return
    if reducer_scaler < 2:
        print("Reducer scaler must be at least 2")
        return
    with open("docker-compose.yaml", 'r') as f:
        doc = yaml.safe_load(f)
        doc["services"]["server"]["environment"][1] = f"CONNECTED_NODES={node_scaler}"
        doc["services"]["initial_column_cleaner_1"]["environment"][4] = f"CONNECTED_NODES={node_scaler}"
        doc["services"]["group_by_route"]["environment"][1] = f"REDUCERS_AMOUNT={reducer_scaler}"
        doc["services"]["group_by_airport"]["environment"][1] = f"REDUCERS_AMOUNT={reducer_scaler}"
        doc["services"]["group_by_route_query_4"]["environment"][1] = f"REDUCERS_AMOUNT={reducer_scaler}"
        doc["services"]["query_handler"]["environment"][1] = f"TOTAL_REDUCERS={reducer_scaler}"
        doc["services"]["group_by_route_query_4"]["environment"][2] = f"EOF_REQUIRED={node_scaler}"
        for node in nodes_with_dependencies:
            add_depends(doc["services"][node]["depends_on"], node_scaler, reducer_scaler)
        for node in nodes_to_scale:
            multiply_node(doc["services"], node_scaler, node)
        for reducer in reducers_to_scale:
            multiply_reducers(doc["services"], reducer_scaler, reducer)
    with open("docker-compose-scaled.yaml", 'w') as f:
        yaml.dump(doc, f, sort_keys=False)


if __name__ == "__main__":
    main()
