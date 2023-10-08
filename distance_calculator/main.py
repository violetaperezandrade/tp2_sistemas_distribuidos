from distance_calculator import DistanceCalculator


def main():
    query_handler = DistanceCalculator("distance_calculation",
                                       "registers_with_distance")
    query_handler.run()


if __name__ == '__main__':
    main()
