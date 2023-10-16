import boto3
import botocore
import difflib
import re
import yaml
import os

# Initialize the AWS CodeCommit client
client = boto3.client('codecommit')
dynamodb = boto3.client('dynamodb')


def get_added_and_removed_content(codecommit_repo_name, before_commit_id, after_commit_id):
    added_content = {}
    removed_content = {}
    newfile_content = {}

    try:
        differences = client.get_differences(
            repositoryName=codecommit_repo_name,
            beforeCommitSpecifier=before_commit_id,
            afterCommitSpecifier=after_commit_id
        )['differences']

        for diff in differences:
            file_path = diff.get('afterBlob', {}).get('path', '')

            if not file_path:
                continue

            try:
                file_exists_response = client.get_file(
                repositoryName=codecommit_repo_name,
                commitSpecifier=after_commit_id,
                filePath=file_path
                )
                
                after_response = client.get_file(
                    repositoryName=codecommit_repo_name,
                    commitSpecifier=after_commit_id,
                    filePath=file_path
                )
                after_content = after_response['fileContent'].decode('utf-8').splitlines()
                
                before_response = client.get_file(
                    repositoryName=codecommit_repo_name,
                    commitSpecifier=before_commit_id,
                    filePath=file_path
                )
                before_content = before_response['fileContent'].decode('utf-8').splitlines()
                
                diff = difflib.unified_diff(before_content, after_content, lineterm='', fromfile='Before', tofile='After')
                
                added_changes = []
                removed_changes = []
                
                for line in diff:
                    if line.startswith('+ '):
                        added_changes.append(line[2:])
                    elif line.startswith('- '):
                        removed_changes.append(line[2:])
                
                if added_changes:
                    added_content[file_path] = added_changes
                if removed_changes:
                    removed_content[file_path] = removed_changes
               
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'FileDoesNotExistException':
                    # File does not exist in the repository at after_commit_id, so it's newly added
                    newfile_content[file_path] = []
                else:
                    print(f"Error processing file {file_path}: {str(e)}")

        return added_content, removed_content, newfile_content

    except botocore.exceptions.ClientError as e:
        print(f"Error getting differences: {str(e)}")
        return None, None

def find_function_names(changed_files):
    function_names = {}  # Use a dictionary to store file names and their associated function names

    for file_name, content_changes in changed_files.items():
        unique_function_names = set()  # Use a set to store unique function names for each file

        for line in content_changes:
            matches = re.findall(r'FunctionName:\s*(\S+)', line)
            if matches:
                # Extract function names using regular expression and add them to the set
                unique_function_names.update(matches)

        # Update the dictionary with the file name and associated function names
        function_names[file_name] = list(unique_function_names)

    return function_names
    
def get_previous_commit_id(repository_name, branch_name, commit_id):
    # Get information about the current commit
    response = client.get_commit(
        repositoryName=repository_name,
        commitId=commit_id
    )

    # Check if there are parent commits
    if 'parents' in response['commit'] and len(response['commit']['parents']) > 0:
        # Return the commit ID of the first parent, which is the previous commit
        return response['commit']['parents'][0]
    else:
        # There are no parent commits, so this is either the initial commit or an error
        return None
def is_yaml_file(file_path):
    return file_path.lower().endswith(('.yml', '.yaml'))
    
# def extract_filename(file_path):
#     # Normalize the file path to handle different separators and formats
#     file_path = os.path.normpath(file_path)
    
#     # Split the file path by the separator (e.g., '/')
#     path_parts = file_path.split(os.sep)
    
#     # Iterate over the path parts and find the part with the ".yml" or ".yaml" extension
#     for part in reversed(path_parts):
#         if part.endswith(('.yml', '.yaml')):
#             return part

def read_file_content(codecommit_repo_name, commitSpecifier, file_path):
    codecommit = boto3.client('codecommit')

    try:
        if is_yaml_file(file_path):
            response = codecommit.get_file(
                repositoryName=codecommit_repo_name,
                commitSpecifier=commitSpecifier,
                filePath=file_path
            )
    
            # Extract the content from the response as bytes
            file_content_bytes = response['fileContent']
    
            # Decode the base64-encoded bytes content to a string
            file_content = file_content_bytes.decode('utf-8')
    
            return file_content
    
    except Exception as e:
        # Handle exceptions if the file cannot be read
        return str(e)
    
def extract_resource_names(yaml_content):
    try:
        # Parse the YAML content
        parsed_yaml = yaml.safe_load(yaml_content)

        # Assuming the resources are under a 'Resources' key
        resources = parsed_yaml.get('Resources', {})

        return resources  # Return the entire 'Resources' dictionary
    except Exception as e:
        return str(e)

def content_empty(removed_content):
    for key, value in removed_content.items():
        if value and any(val.strip() for val in value):
            return False
    return True

def extract_function_names(removed_content):
    function_names = []
    for content in removed_content:
        # Split the content by lines and iterate through each line
        for line in removed_content[content]:
            # Check if the line contains 'FunctionName' and extract the value
            if 'FunctionName' in line:
                # Extract the function name (assuming it's after 'FunctionName:')
                function_name = line.split(':')[-1].strip()
                function_names.append(function_name)

    return function_names

def does_table_exist(table_name):
    try:
        dynamodb.describe_table(TableName=table_name)
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        return False
        
def create_table(table_name):
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'file_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'file_name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        return {
            'statusCode': 200,
            'body': 'Table created successfully'
        }
    except Exception as e:
        print("reason:", e)
        return {
            'statusCode': 500,
            'body': 'Error occurred while creating the table'
        }
def create_item(table_name, file_name, folder_name, function_name):
    try:
        # Create the item
        response = dynamodb.put_item(
            TableName=table_name,
            Item={
                'file_name': {'S': file_name},
                'folder_name': {'S': folder_name},
                'function_name': {'S': function_name}
            }
        )
        return {
            'statusCode': 200,
            'body': 'Item created successfully'
        }
    except Exception as e:
        print("reason:", e)
        return {
            'statusCode': 500,
            'body': 'Error occurred while creating the item'
        }
def does_file_exist(table_name, file_name):
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={'file_name': {'S': file_name}}
        )
        return 'Item' in response
    except dynamodb.exceptions.ResourceNotFoundException:
        return False
        
def add_function_to_file_name_item(table_name, file_name, function_name):
    # Step 1: Retrieve current function names from DynamoDB item
    response = dynamodb.get_item(
        TableName=table_name,
        Key={'file_name': {'S': file_name}},
        ProjectionExpression='function_name'
    )

    # Initialize the list of function names
    existing_functions = ""

    # Check if the item exists and contains function names
    if 'Item' in response:
        existing_functions = response['Item'].get('function_name', {}).get('S', "")

    # Step 2: Append the new function name to the existing list
    if existing_functions:
        updated_functions = f"{existing_functions},{function_name}"
    else:
        updated_functions = function_name

    # Step 3: Update the DynamoDB item with the updated list
    dynamodb.update_item(
        TableName=table_name,
        Key={'file_name': {'S': file_name}},
        UpdateExpression='SET #function_name = :function_name',
        ExpressionAttributeNames={'#function_name': 'function_name'},
        ExpressionAttributeValues={':function_name': {'S': updated_functions}}
    )
def function_exists(table_name, file_name, function_name):
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={'file_name': {'S': file_name}},
            ProjectionExpression='function_name'
        )

        # Check if the item exists and contains function names
        if 'Item' in response:
            existing_functions = response['Item'].get('function_name', {}).get('S', "")
            return function_name in existing_functions.split(',')

        return False
    except dynamodb.exceptions.ResourceNotFoundException:
        return False
    except Exception as e:
        print("reason:", e)
        return False
def remove_function_from_items(search_value):
    dynamodb = boto3.client('dynamodb')
    function_name_attribute_name = 'function_name'
    file_name_attribute_name = 'file_name'

    # List all DynamoDB tables
    table_names = dynamodb.list_tables()['TableNames']

    for table_name in table_names:
        try:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='contains(#func_attr, :value)',
                ExpressionAttributeNames={'#func_attr': function_name_attribute_name},
                ExpressionAttributeValues={':value': {'S': search_value}}
            )

            if 'Items' in response and len(response['Items']) > 0:
                print(f"Found matching items in table {table_name}:")
                for item in response['Items']:
                    if file_name_attribute_name in item:
                        file_name_value = item[file_name_attribute_name]['S']
                        print(f"File Name: {file_name_value}")
                        remove_function(table_name, file_name_value, search_value)
                    print("Item:", item)
            else:
                print(f"No matching item found in table {table_name}")

        except Exception as e:
            print(f"An error occurred while scanning table {table_name}: {e}")

def remove_function(table_name, partition_key_value, value_to_remove):
    dynamodb = boto3.client('dynamodb')
    attribute_name = 'function_name'

    try:
        primary_key = {'file_name': {'S': partition_key_value}}
        response = dynamodb.get_item(TableName=table_name, Key=primary_key)

        if 'Item' in response:
            item = response['Item']

            if attribute_name in item:
                comma_separated_string = item[attribute_name]['S']
                values_list = comma_separated_string.split(',')

                if value_to_remove in values_list:
                    values_list.remove(value_to_remove)
                    updated_string = ','.join(values_list)

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
        
def add_added_content(codecommit_repo_name,commit_id,filenames):
    
    for file_name in filenames:
        parts = file_name.split('/')
        modified_file_path = parts[-1].split('.')[0]
        print(f"Modified file: {modified_file_path}")
        # Attempt to read the file content
        # modified_file_path = extract_filename(file_name)
        cloudformation_template = read_file_content(codecommit_repo_name, commit_id, file_name)
        # print("cloudformation_template",cloudformation_template)
        if cloudformation_template is None:
            print(f"Failed to read file: {file_name}")
            continue

        yaml_content = re.sub(r'!(\w+)\s+[^\s]+', 'dummy-value', cloudformation_template)
        resources = extract_resource_names(yaml_content)
        print(resources)

        for resource_name, resource_properties in resources.items():
            print("resource_properties", resource_properties)
            if 'Handler' in resource_properties.get('Properties', {}):
                try:
                    handler_value = resource_properties['Properties']['Handler']
                    handler_value = handler_value.split('.')[0]
                    FunctionName = resource_properties['Properties']['FunctionName']
                    print("FunctionName",FunctionName)
                    print(f"Resource: {resource_name}, Handler: {handler_value}")

                    # Assuming handler_value is the table name and function_name is the item's function_name
                    table_name = handler_value
                    file_name = handler_value
                    folder_name = modified_file_path
                    function_name = FunctionName

                    # Check if the table exists
                    table_exists = does_table_exist(table_name)

                    if not table_exists:
                        # Create the table if it doesn't exist
                        create_table(table_name)
                        time.sleep(8)
                        print(f"Table '{table_name}' created successfully")

                        # Create the item immediately after table creation
                        create_item(table_name, file_name, folder_name, function_name)
                        print(f"Item '{file_name}' inserted successfully")

                    # Check if the file exists
                    file_exists = does_file_exist(table_name, file_name)

                    if not file_exists:
                        # Create the item if it doesn't exist
                        create_item(table_name, file_name, folder_name, function_name)
                        print(f"Item '{file_name}' inserted successfully")
                    else:
                        # Check if the function exists for the file
                        if not function_exists(table_name, file_name, function_name):
                            add_function_to_file_name_item(table_name, file_name, function_name)
                            print(f"Function '{function_name}' added to item '{file_name}' successfully")

                    return {
                        'statusCode': 200,
                        'body': 'Operation completed successfully'
                    }

                except Exception as e:
                    print("reason:", e)
                    return {
                        'statusCode': 500,
                        'body': 'Error occurred'
                    }
        
def lambda_handler(event, context):
    print("event",event)
    try:
        # Extract necessary information from the event
        aws_region = event['Records'][0]['awsRegion']
        codecommit_repo_arn = event['Records'][0]['eventSourceARN']
        codecommit_repo_name = codecommit_repo_arn.split(':')[5]
        branch_name = event['Records'][0]['codecommit']['references'][0]['ref'].split('heads/')[-1]
        commit_id = event['Records'][0]['codecommit']['references'][0]['commit']
        repository = event['Records'][0]['eventSourceARN'].split(':')[5]

        # Initialize CodeCommit client
        codecommit_client = boto3.client('codecommit', region_name=aws_region)

        # Get the last commit
        last_commit = get_previous_commit_id(repository, branch_name, commit_id)

        # Get added and removed content
        added_content, removed_content, newfile_content = get_added_and_removed_content(repository, last_commit, commit_id)
        print("added_content", added_content)
        print("removed_content", removed_content)

        if not content_empty(removed_content): 
            function_names_for_remove = extract_function_names(removed_content)
            for function_name in function_names_for_remove:
                print("item",function_name)
                remove_function_from_items(function_name)
        
        if not content_empty(added_content):

            function_names = find_function_names(added_content)
            print("function_names", function_names)
            filenames = list(function_names.keys())
            add_added_content(codecommit_repo_name,commit_id,filenames)
        if newfile_content:
            filenames = list(newfile_content.keys())
            print("#########",filenames)
            add_added_content(codecommit_repo_name,commit_id,filenames)
            # filenames = list(function_names.keys())
            # for file_name in filenames:
            #     parts = file_name.split('/')
            #     modified_file_path = parts[-1].split('.')[0]
            #     print(f"Modified file: {modified_file_path}")

            #     # Attempt to read the file content
            #     cloudformation_template = read_file_content(codecommit_repo_name, commit_id, file_name)
            #     # print("cloudformation_template",cloudformation_template)
            #     if cloudformation_template is None:
            #         print(f"Failed to read file: {file_name}")
            #         continue

            #     yaml_content = re.sub(r'!(\w+)\s+[^\s]+', 'dummy-value', cloudformation_template)
            #     resources = extract_resource_names(yaml_content)
            #     print(resources)

            #     for resource_name, resource_properties in resources.items():
            #         print("resource_properties", resource_properties)
            #         if 'Handler' in resource_properties.get('Properties', {}):
            #             try:
            #                 handler_value = resource_properties['Properties']['Handler']
            #                 handler_value = handler_value.split('.')[0]
            #                 FunctionName = resource_properties['Properties']['FunctionName']
            #                 print("FunctionName",FunctionName)
            #                 print(f"Resource: {resource_name}, Handler: {handler_value}")

            #                 # Assuming handler_value is the table name and function_name is the item's function_name
            #                 table_name = handler_value
            #                 file_name = handler_value
            #                 folder_name = modified_file_path
            #                 function_name = FunctionName

            #                 # Check if the table exists
            #                 table_exists = does_table_exist(table_name)

            #                 if not table_exists:
            #                     # Create the table if it doesn't exist
            #                     create_table(table_name)
            #                     time.sleep(8)
            #                     print(f"Table '{table_name}' created successfully")

            #                     # Create the item immediately after table creation
            #                     create_item(table_name, file_name, folder_name, function_name)
            #                     print(f"Item '{file_name}' inserted successfully")

            #                 # Check if the file exists
            #                 file_exists = does_file_exist(table_name, file_name)

            #                 if not file_exists:
            #                     # Create the item if it doesn't exist
            #                     create_item(table_name, file_name, folder_name, function_name)
            #                     print(f"Item '{file_name}' inserted successfully")
            #                 else:
            #                     # Check if the function exists for the file
            #                     if not function_exists(table_name, file_name, function_name):
            #                         add_function_to_file_name_item(table_name, file_name, function_name)
            #                         print(f"Function '{function_name}' added to item '{file_name}' successfully")

            #                 return {
            #                     'statusCode': 200,
            #                     'body': 'Operation completed successfully'
            #                 }

            #             except Exception as e:
            #                 print("reason:", e)
            #                 return {
            #                     'statusCode': 500,
            #                     'body': 'Error occurred'
            #                 }

    except Exception as e:
        print("An error occurred:", str(e))
        return {
            'statusCode': 500,
            'body': 'Error occurred'
        }

