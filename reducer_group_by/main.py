from reducer_group_by import ReducerGroupBy
import os
import pika


def main():
    field_group_by = os.environ['FIELD_GROUP_BY']
    output_queue = os.environ['OUTPUT_QUEUE']
    input_queue = os.environ['INPUT_QUEUE']
    query_number = int(os.environ['QUERY_NUMBER'])
    name = os.environ['HOSTNAME']
    reducer_group_by = ReducerGroupBy(field_group_by, input_queue,
                                      output_queue, query_number,
                                      name)
    try:
        reducer_group_by.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
