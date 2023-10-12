from distance_calculator import DistanceCalculator
import pika


def main():
    query_handler = DistanceCalculator("airport_registers",
                                       "distance_calculation",
                                       "output_2")
    try:
        query_handler.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
