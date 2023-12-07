from subprocess import run


def main():
    results_size = ["500K"]
    clients = ["client_1", "client_2", "client_3"]
    queries = {"query1", "query2", "query3", "query4", "query5"}
    for index, result_size in enumerate(results_size):
        for query in queries:
            try:
                with (open(f"correct_results/{query}_{result_size}.txt", "r") as correct_file,
                      open(f"results/{clients[index]}/{query}.txt", "r") as test_file):
                    output = run(f"diff <(sort {test_file}) <(sort {correct_file})", capture_output=True).stdout
                    if len(output) > 0:
                        print(output)
                    else:
                        print(f"Los archivos de {query} para {result_size} coinciden. ")
            except FileNotFoundError:
                print(f"No encontre los archivos requeridos para {query} de {result_size}")
                continue


if __name__ == "__main__":
    main()
