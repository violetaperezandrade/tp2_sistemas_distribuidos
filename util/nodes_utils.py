# NODES_R = ["group_by_id", "initial_column_cleaner",
#            "filter_by_three_stopovers", "reducer_group_by_route",
#            "group_by_id", "healthchecker"]
# NODES_U = ["group_by_route", "query_handler"]

NODES_R = ["healthchecker", "initial_column_cleaner",
           "group_by_id", "filter_by_three_stopovers"]
NODES_U = ["query_handler"]

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
