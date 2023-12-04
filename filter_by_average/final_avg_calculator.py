import json
import os

from util.constants import NUMBER_CLIENTS, AVG_READY
from util.file_manager import log_to_file
from util.initialization import initialize_queues, initialize_exchanges
from util.queue_middleware import QueueMiddleware
from util.recovery_logging import correct_last_line


class FinalAvgCalculator:
    def __init__(self, avg_exchange, exchange_queue, id, name, total_reducers, pipe):
        self.__avg_exchange = avg_exchange
        self.__middleware = QueueMiddleware()
        self.__total_reducers = total_reducers
        self.__id = id
        self.__pipe = pipe
        self.__name = name
        self.__exchange_queue = f"{exchange_queue}_{id}"
        self.main_path = f"filter_by_average/{self.__name}"
        self.__sent_eof = {i: False for i in range(1, NUMBER_CLIENTS + 1)}

    def run(self):
        initialize_queues([self.__exchange_queue], self.__middleware)
        initialize_exchanges([self.__avg_exchange], self.__middleware)
        self.recover_state()
        os.makedirs(self.main_path, exist_ok=True)
        self.__middleware.subscribe(self.__avg_exchange, self.callback_avg, self.__exchange_queue)

    def callback_avg(self, body, method):
        flight = json.loads(body)
        client_id = int(flight["client_id"])
        sum = float(flight["sum"])
        count = int(flight["count"])
        if self.__sent_eof[client_id]:
            self.__middleware.manual_ack(method)
            return
        log_to_file(self.get_avg_file(client_id),
                    f"{sum},{count}")
        self.send_avg_if_complete(client_id)
        self.__middleware.manual_ack(method)
        return

    def send_avg_if_complete(self, client_id):
        filepath = self.get_avg_file(client_id)
        sum = 0
        count = 0
        valid_lines = 0
        if os.path.exists(filepath):
            correct_last_line(filepath)
            with open(filepath, 'r') as file:
                for line in file:
                    if line.endswith("#\n"):
                        continue
                    try:
                        sum_temp, count_temp = tuple(line.split(","))
                    except ValueError as e:
                        continue
                    valid_lines += 1
                    sum += float(sum_temp)
                    count += int(count_temp)
            if valid_lines == self.__total_reducers:
                total_avg_message = self.create_total_avg_message(client_id, sum, count)
                self.__sent_eof[client_id] = True
                self.__pipe.send(total_avg_message)

    def recover_state(self):
        for i in range(1, NUMBER_CLIENTS + 1):
            self.send_avg_if_complete(i)

    def create_total_avg_message(self, client_id, sum, count):
        total_avg = dict()
        total_avg["op_code"] = AVG_READY
        total_avg["client_id"] = int(client_id)
        total_avg["avg"] = sum / count
        return total_avg

    def get_avg_file(self, client_id):
        file = f"{self.main_path}/client_{client_id}_avg_log.txt"
        return file
