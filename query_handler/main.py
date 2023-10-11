import multiprocessing
import os

from query_handler import QueryHandler
QUERIES = 5


def run(query_number):
    reducers = int(os.environ['TOTAL_REDUCERS'])
    if query_number in [1, 2]:
        reducers = 1
    query_handler = QueryHandler(query_number, reducers)
    query_handler.run()


processes = []
for i in range(1, QUERIES+1):
    process = multiprocessing.Process(target=run, args=(i,))
    processes.append(process)
    process.start()

for process in processes:
    process.join()
