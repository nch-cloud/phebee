from phebee.utils.aws import get_client


def get_output_value_from_stack(stack_name, output_key):
    """Retrieve the ARN of the Step Function from the CloudFormation stack output."""
    cf_client = get_client("cloudformation")
    response = cf_client.describe_stacks(StackName=stack_name)

    outputs = response["Stacks"][0]["Outputs"]

    for output in outputs:
        if output["OutputKey"] == output_key:
            return output["OutputValue"]
    raise Exception(f"Output value for key '{output_key}' not found in stack outputs.")
