from group_by import GroupBy
import os
import pika


def main():
    field_group_by = os.environ['FIELD_GROUP_BY'].split(",")
    input_exchange = os.environ['INPUT_EXCHANGE']
    reducers_amount = int(os.environ['REDUCERS_AMOUNT'])
    queue_group_by = os.environ['QUEUE_GROUP_BY']
    input_queue = os.environ['INPUT_QUEUE']
    listening_queue = os.environ['LISTENING_QUEUE']
    required_eof = int(os.getenv('EOF_REQUIRED', 1))

    group_by = GroupBy(field_group_by, input_exchange,
                       reducers_amount, queue_group_by, listening_queue,
                       input_queue, required_eof)
    try:
        group_by.run()
    except pika.exceptions.ChannelWrongStateError:
        pass


if __name__ == '__main__':
    main()
