from group_by import GroupBy
import os


def initialize_config():
    config_params = {}
    try:
        config_params["field_group_by"] = os.environ['FIELD_GROUP_BY'].split(",")
        config_params["input_exchange"] = os.environ['INPUT_EXCHANGE']
        config_params["reducers_amount"] = int(os.environ['REDUCERS_AMOUNT'])
        config_params["queue_group_by"] = os.environ['QUEUE_GROUP_BY']
        config_params["input_queue"] = os.environ['INPUT_QUEUE']
        config_params["listening_queue"] = os.environ['LISTENING_QUEUE']
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    field_group_by = config_params["field_group_by"]
    input_exchange = config_params["input_exchange"]
    reducers_amount = config_params["reducers_amount"]
    queue_group_by = config_params["queue_group_by"]
    input_queue = config_params["input_queue"]
    listening_queue = config_params["listening_queue"]
    group_by = GroupBy(field_group_by, input_exchange,
                       reducers_amount, queue_group_by, listening_queue,
                       input_queue)

    group_by.run()


if __name__ == '__main__':
    main()
