from fastests_calculator import FastestsCalculator
import os


def initialize_config():
    config_params = {}
    try:
        config_params["grouped_by_queue"] = os.environ["GROUPED_BY_QUEUE"]
        config_params["output_queue"] = os.environ["OUTPUT_QUEUE"]
        config_params["reducers_amount"] = os.environ["REDUCERS_AMOUNT"]
        config_params["result_fields"] = os.environ["RESULT_FIELDS"].split(",")
        config_params["duration_field"] = os.environ["DURATION_FIELD"]
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {} .Aborting client".format(e))
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting client".format(e))

    return config_params


def main():

    config_params = initialize_config()
    grouped_by_queue = config_params["grouped_by_queue"]
    output_queue = config_params["output_queue"]
    reducers_amount = config_params["reducers_amount"]
    result_fields = config_params["result_fields"]
    duration_field = config_params["duration_field"]

    fastests_calculator = FastestsCalculator(grouped_by_queue, output_queue,
                                             reducers_amount, result_fields,
                                             duration_field)

    fastests_calculator.run()


if __name__ == '__main__':
    main()
