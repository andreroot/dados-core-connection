import sys
def lambda_handler(event, context):
    print('teste')
    return 'Hello from AWS Lambda using Python' + sys.version + '!'