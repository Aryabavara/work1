import boto3

# Initialize the AWS CodeCommit client
client = boto3.client('codecommit')

def remove_function(table_name, partition_key_value, value_to_remove):
    
    # Initialize the DynamoDB client
    dynamodb = boto3.client('dynamodb')
    
    # Value to remove from the comma-separated string
    # value_to_remove = 'listBugs'
    
    # Attribute name to update
    attribute_name = 'function_name'
    
    try:
        # Construct the primary key
        primary_key = {'file_name': {'S': partition_key_value}}
        
        # Retrieve the item from DynamoDB
        response = dynamodb.get_item(TableName=table_name, Key=primary_key)
        
        # Check if the item exists
        if 'Item' in response:
            item = response['Item']
            
            # Extract the comma-separated string from the item
            if attribute_name in item:
                comma_separated_string = item[attribute_name]['S']
                
                # Split the string into a list
                values_list = comma_separated_string.split(',')
                
                # Remove the value you want to delete
                if value_to_remove in values_list:
                    values_list.remove(value_to_remove)
                    
                    # Join the list back into a string
                    updated_string = ','.join(values_list)
                    
                    # Update the item in DynamoDB with the modified string
                    dynamodb.update_item(
                        TableName=table_name,
                        Key=primary_key,
                        UpdateExpression=f'SET {attribute_name} = :updated_string',
                        ExpressionAttributeValues={':updated_string': {'S': updated_string}}
                    )
                    print(f"Removed '{value_to_remove}' from '{attribute_name}' in table {table_name}")
                else:
                    print(f"'{value_to_remove}' not found in '{attribute_name}' in table {table_name}")
            else:
                print(f"'{attribute_name}' not found in item in table {table_name}")
        else:
            print(f"Item not found in table {table_name}")
    except Exception as e:
        print(f"An error occurred: {e}")


def lambda_handler(event, context):
    # Initialize the DynamoDB client
    dynamodb = boto3.client('dynamodb')
    
    # List all DynamoDB tables
    table_names = dynamodb.list_tables()['TableNames']
    
    # Value to search for
    search_value = 'listBugs'
    
    # Attribute names to search in
    function_name_attribute_name = 'function_name'
    file_name_attribute_name = 'file_name'
    
    # Iterate through each table and perform a scan
    for table_name in table_names:
        try:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='contains(#func_attr, :value)',
                ExpressionAttributeNames={'#func_attr': function_name_attribute_name},
                ExpressionAttributeValues={':value': {'S': search_value}}
            )
            
            # Check if any items match the search in the current table
            if 'Items' in response and len(response['Items']) > 0:
                print(f"Found matching items in table {table_name}:")
                for item in response['Items']:
                    # Check if the file_name attribute exists in the item
                    if file_name_attribute_name in item:
                        file_name_value = item[file_name_attribute_name]['S']
                        print(f"File Name: {file_name_value}")
                        remove_function(table_name, file_name_value, search_value)
                    print("Item:", item)
            else:
                print(f"No matching items found in table {table_name}")
        except Exception as e:
            print(f"An error occurred while scanning table {table_name}: {e}")

# import boto3
# import botocore
# client = boto3.client('codecommit')
# dynamodb = boto3.client('dynamodb')
# def lambda_handler(event, context):

#     try:
#         table_name = "bavara"
#         file_name = "shaji"
#         folder_name = "baiju"
#         function_name = "kichu"
      
#         table_exists = does_table_exist(table_name)
#         file_exists = does_file_exist(table_name, file_name)

#         if not table_exists:
#             # Create the table if it doesn't exist
#             create_table(table_name)
#             time.sleep(5)
#             print(f"Table '{table_name}' created successfully")
            
#             # Create the item immediately after table creation
#             create_item(table_name, file_name, folder_name, function_name)
#             print(f"Item '{file_name}' inserted successfully")

#         if not file_exists:
#             # Create the item if it doesn't exist
#             create_item(table_name, file_name, folder_name, function_name)
#             print(f"Item '{file_name}' inserted successfully")
#         else:
#             if not function_exists(table_name, file_name, function_name):
#                 add_function_to_file_name_item(table_name, file_name, function_name)
#                 print(f"Function '{function_name}' added to item '{file_name}' successfully")

#         return {
#             'statusCode': 200,
#             'body': 'Operation completed successfully'
#         }
#     except Exception as e:
#         print("reason:", e)
#         return {
#             'statusCode': 500,
#             'body': 'Error occurred'
#         }

# def does_table_exist(table_name):
#     try:
#         dynamodb.describe_table(TableName=table_name)
#         return True
#     except dynamodb.exceptions.ResourceNotFoundException:
#         return False

# def does_file_exist(table_name, file_name):
#     try:
#         response = dynamodb.get_item(
#             TableName=table_name,
#             Key={'file_name': {'S': file_name}}
#         )
#         return 'Item' in response
#     except dynamodb.exceptions.ResourceNotFoundException:
#         return False
        
# def create_table(table_name):
#     try:
#         dynamodb.create_table(
#             TableName=table_name,
#             KeySchema=[
#                 {
#                     'AttributeName': 'file_name',
#                     'KeyType': 'HASH'
#                 }
#             ],
#             AttributeDefinitions=[
#                 {
#                     'AttributeName': 'file_name',
#                     'AttributeType': 'S'
#                 }
#             ],
#             ProvisionedThroughput={
#                 'ReadCapacityUnits': 5,
#                 'WriteCapacityUnits': 5
#             }
#         )
#         return {
#             'statusCode': 200,
#             'body': 'Table creation request sent successfully'
#         }
#     except Exception as e:
#         print("reason:", e)
#         return {
#             'statusCode': 500,
#             'body': 'Error occurred while creating the table'
#         }

# def wait_for_table_creation(table_name):
#     dynamodb.get_waiter('table_exists').wait(
#         TableName=table_name,
#         WaiterConfig={
#             'Delay': 5,       # Increase the delay if necessary
#             'MaxAttempts': 60  # Increase the max attempts if necessary
#         }
#     )
# def create_item(table_name, file_name, folder_name, function_name):
#     try:
#         # Create the item
#         response = dynamodb.put_item(
#             TableName=table_name,
#             Item={
#                 'file_name': {'S': file_name},
#                 'folder_name': {'S': folder_name},
#                 'function_name': {'S': function_name}
#             }
#         )
#         return {
#             'statusCode': 200,
#             'body': 'Item created successfully'
#         }
#     except Exception as e:
#         print("reason:", e)
#         return {
#             'statusCode': 500,
#             'body': 'Error occurred while creating the item'
#         }

# def add_function_to_file_name_item(table_name, file_name, function_name):
#     try:
#         # Step 1: Retrieve current function names from DynamoDB item
#         response = dynamodb.get_item(
#             TableName=table_name,
#             Key={'file_name': {'S': file_name}},
#             ProjectionExpression='function_name'
#         )

#         # Initialize the list of function names
#         existing_functions = ""

#         # Check if the item exists and contains function names
#         if 'Item' in response:
#             existing_functions = response['Item'].get('function_name', {}).get('S', "")

#         # Step 2: Append the new function name to the existing list
#         if existing_functions:
#             updated_functions = f"{existing_functions},{function_name}"
#         else:
#             updated_functions = function_name

#         # Step 3: Update the DynamoDB item with the updated list
#         dynamodb.update_item(
#             TableName=table_name,
#             Key={'file_name': {'S': file_name}},
#             UpdateExpression='SET #function_name = :function_name',
#             ExpressionAttributeNames={'#function_name': 'function_name'},
#             ExpressionAttributeValues={':function_name': {'S': updated_functions}}
#         )

#         return {
#             'statusCode': 200,
#             'body': 'add_function_to_file_name_item updated successfully'
#         }
#     except Exception as e:
#         print("reason:", e)
#         return {
#             'statusCode': 500,
#             'body': 'Error occurred while updating function names'
#         }
