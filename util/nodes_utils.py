NODES_R = ["healthchecker", "initial_column_cleaner",
           "group_by_id", "filter_by_three_stopovers",
           "query_2_column_filter",
           "distance_calculator", "query_5_column_filter",
           "reducer_group_by_airport",
           "reducer_group_by_route", "avg_calculator",
           "filter_by_average", "group_by_id_avg",
           "reducer_group_by_route_q4"]
NODES_U = ["query_handler", "group_by_airport", "group_by_route",
           "group_by_route_query_4"]

REPLICATED_NODES = 3


def get_nodes_list():
    nodes_list = []
    for node in NODES_R:
        for i in range(1, REPLICATED_NODES + 1):
            nodes_list.append(f"{node}_{str(i)}")
    nodes_list += NODES_U
    return nodes_list


def get_node_from_idx(node_id):
    nodes_list = get_nodes_list()
    return nodes_list[node_id]
