import os
import logging
from healthchecker import HealthChecker


def main():
    id = int(os.environ['ID'])
    name = os.environ['HOSTNAME']
    total_amount = int(os.environ['TOTAL_AMOUNT'])
    logging_level = os.environ["LOGGING_LEVEL"]
    nodes_idxs = os.environ["NODES_IDXS"].split(",")
    nodes_timeout = float(os.environ['NODES_TIMEOUT'])
    frequency = int(os.environ['FREQUENCY'])
    initialize_log(logging_level)
    health_checker = HealthChecker(id, total_amount, name,
                                   nodes_idxs, nodes_timeout,
                                   frequency)
    health_checker.start()


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
