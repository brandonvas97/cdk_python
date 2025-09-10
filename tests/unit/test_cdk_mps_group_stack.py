import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_mps_group.cdk_mps_group_stack import CdkMpsGroupStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cdk_mps_group/cdk_mps_group_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkMpsGroupStack(app, "cdk-mps-group")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
