import multiprocessing
from query_handler import QueryHandler
QUERIES = 4


def run(query_number):
    query_handler = QueryHandler(query_number)
    query_handler.run()


processes = []
for i in range(1, QUERIES+1):
    process = multiprocessing.Process(target=run, args=(i, ))
    processes.append(process)
    process.start()

for process in processes:
    process.join()
