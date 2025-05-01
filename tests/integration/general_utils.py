from phebee.utils.aws import get_current_timestamp


def check_timestamp_in_test(timestamp, test_start_time):
    return timestamp and timestamp > test_start_time and timestamp < get_current_timestamp()
