import boto3
import os

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId

load_dotenv()


class DatabaseService(ABC):
    @abstractmethod
    def insert(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def find(self, query: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    def update(self, query: Dict[str, Any], update_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def delete(self, query: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def get_by_id(self, id_value: Any, **kwargs) -> Optional[Dict[str, Any]]:
        pass


class DynamoDBService(DatabaseService):
    """
    DynamoDB implementation tailored to this project's needs.

    Assumptions:
    - The table has a partition key named 'job_id' (string).
    - Items are stored as plain attribute maps (no nested DynamoDB types).
    - We primarily:
        * Insert full job documents
        * Update by {'job_id': <id>}
        * Find by {'job_id': <id>} or all jobs ({}), optionally with
          limit/skip/sort on 'created_at'.
    """

    def __init__(self, table_name: str, region_name: str = 'ap-south-2', **config):
        self.table_name = table_name
        self.client = boto3.client('dynamodb', region_name=region_name, **config)
        self.resource = boto3.resource('dynamodb', region_name=region_name, **config)
        self.table = self.resource.Table(table_name)

        print(
            f"DynamoDBService initialized with table_name: {table_name}, "
            f"region_name: {region_name}, config: {config}"
        )

        # Ensure the table exists; create it if missing
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """
        Ensure the DynamoDB table exists. If not, create it with:
        - Partition key: job_id (string)
        """
        try:
            self.client.describe_table(TableName=self.table_name)
            # Table exists
            return
        except self.client.exceptions.ResourceNotFoundException:
            print(f"Creating DynamoDB table '{self.table_name}' for jobs...")
            try:
                self.client.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {"AttributeName": "job_id", "KeyType": "HASH"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "job_id", "AttributeType": "S"},
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
                # Wait until the table exists
                waiter = self.client.get_waiter("table_exists")
                waiter.wait(TableName=self.table_name)
                print(f"DynamoDB table '{self.table_name}' created successfully.")
            except ClientError as e:
                raise RuntimeError(f"Failed to create DynamoDB table '{self.table_name}': {e}") from e
    
    def insert(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            # Only create a new item if it does NOT already exist.
            # We assume 'job_id' is the partition key for this project's table.
            response = self.table.put_item(
                Item=data,
                ConditionExpression="attribute_not_exists(job_id)",
                **kwargs,
            )
            return {"success": True, "response": response}
        except ClientError as e:
            # If the item already exists, DynamoDB raises a ConditionalCheckFailedException
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise RuntimeError("DynamoDB insert failed: item with this job_id already exists") from e
            raise RuntimeError(f"DynamoDB insert failed: {e}") from e
    
    def find(self, query: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Simplified find for this project.

        Supported patterns:
        - {}: scan all jobs (with optional limit/skip/sort)
        - {'job_id': <id>}: direct lookup by primary key
        """
        try:
            # Project-level options (not native DynamoDB params)
            limit = kwargs.pop('limit', 0)
            skip = kwargs.pop('skip', 0)
            sort = kwargs.pop('sort', None)

            items: List[Dict[str, Any]] = []

            # Fast-path: lookup by primary key
            if query and set(query.keys()) == {'job_id'}:
                response = self.table.get_item(Key={'job_id': query['job_id']})
                item = response.get('Item')
                if item:
                    items = [item]
            else:
                # Fallback: full table scan
                response = self.table.scan(**kwargs)
                items = response.get('Items', [])

                while 'LastEvaluatedKey' in response:
                    response = self.table.scan(
                        ExclusiveStartKey=response['LastEvaluatedKey'],
                        **kwargs
                    )
                    items.extend(response.get('Items', []))

            # Optional sorting (e.g. sort=[("created_at", -1)])
            if sort and items:
                # Only the first sort key is considered
                field, direction = sort[0]
                reverse = direction < 0
                items.sort(key=lambda d: d.get(field), reverse=reverse)

            # Apply skip/limit in Python
            if skip:
                items = items[skip:]
            if limit:
                items = items[:limit]

            return items
        except ClientError as e:
            raise RuntimeError(f"DynamoDB find failed: {e}")
    
    def update(self, query: Dict[str, Any], update_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            update_expression = kwargs.pop('update_expression', None)
            expression_values = kwargs.pop('expression_values', None)
            
            if not update_expression:
                update_parts = [f"#{k} = :{k}" for k in update_data.keys()]
                update_expression = "SET " + ", ".join(update_parts)
                expression_values = {f":{k}": v for k, v in update_data.items()}
                expression_names = {f"#{k}": k for k in update_data.keys()}
                
                response = self.table.update_item(
                    Key=query,
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values,
                    ExpressionAttributeNames=expression_names,
                    ReturnValues='ALL_NEW',
                    **kwargs
                )
            else:
                response = self.table.update_item(
                    Key=query,
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values,
                    ReturnValues='ALL_NEW',
                    **kwargs
                )
            
            return {'success': True, 'attributes': response.get('Attributes', {})}
        except ClientError as e:
            raise RuntimeError(f"DynamoDB update failed: {e}")
    
    def delete(self, query: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            response = self.table.delete_item(Key=query, **kwargs)
            return {'success': True, 'response': response}
        except ClientError as e:
            raise RuntimeError(f"DynamoDB delete failed: {e}")
    
    def get_by_id(self, id_value: Any, **kwargs) -> Optional[Dict[str, Any]]:
        try:
            key_name = kwargs.pop('key_name', 'id')
            response = self.table.get_item(Key={key_name: id_value}, **kwargs)
            return response.get('Item')
        except ClientError as e:
            raise RuntimeError(f"DynamoDB get_by_id failed: {e}")


class MongoDBService(DatabaseService):
    def __init__(self, database: str, collection: str, 
                 host: str = 'localhost', port: int = 27017, **config):
        connection_string = config.pop('connection_string', None)
        
        if connection_string:
            self.client = MongoClient(connection_string, **config)
        else:
            self.client = MongoClient(host=host, port=port, **config)
        
        self.db = self.client[database]
        self.collection = self.db[collection]
    
    def insert(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            result = self.collection.insert_one(data, **kwargs)
            return {'success': True, 'inserted_id': str(result.inserted_id)}
        except PyMongoError as e:
            raise RuntimeError(f"MongoDB insert failed: {e}")
    
    def find(self, query: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        try:
            projection = kwargs.pop('projection', None)
            limit = kwargs.pop('limit', 0)
            skip = kwargs.pop('skip', 0)
            sort = kwargs.pop('sort', None)
            
            cursor = self.collection.find(query, projection, **kwargs)
            
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            
            results = list(cursor)
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return results
        except PyMongoError as e:
            raise RuntimeError(f"MongoDB find failed: {e}")
    
    def update(self, query: Dict[str, Any], update_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            update_many = kwargs.pop('update_many', False)
            upsert = kwargs.pop('upsert', False)
            
            if not any(k.startswith('$') for k in update_data.keys()):
                update_data = {'$set': update_data}
            
            if update_many:
                result = self.collection.update_many(query, update_data, upsert=upsert, **kwargs)
                return {
                    'success': True,
                    'matched_count': result.matched_count,
                    'modified_count': result.modified_count
                }
            else:
                result = self.collection.update_one(query, update_data, upsert=upsert, **kwargs)
                return {
                    'success': True,
                    'matched_count': result.matched_count,
                    'modified_count': result.modified_count
                }
        except PyMongoError as e:
            raise RuntimeError(f"MongoDB update failed: {e}")
    
    def delete(self, query: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        try:
            delete_many = kwargs.pop('delete_many', False)
            
            if delete_many:
                result = self.collection.delete_many(query, **kwargs)
            else:
                result = self.collection.delete_one(query, **kwargs)
            
            return {'success': True, 'deleted_count': result.deleted_count}
        except PyMongoError as e:
            raise RuntimeError(f"MongoDB delete failed: {e}")
    
    def get_by_id(self, id_value: Any, **kwargs) -> Optional[Dict[str, Any]]:
        try:            
            if isinstance(id_value, str):
                try:
                    id_value = ObjectId(id_value)
                except:
                    pass
            
            result = self.collection.find_one({'_id': id_value}, **kwargs)
            
            if result and '_id' in result:
                result['_id'] = str(result['_id'])
            
            return result
        except PyMongoError as e:
            raise RuntimeError(f"MongoDB get_by_id failed: {e}")
    
    def close(self):
        self.client.close()


class DatabaseWrapper:
    def __init__(self, service_type: str, **config):
        if service_type.lower() == 'dynamodb':
            self.db = DynamoDBService(**config)
        elif service_type.lower() == 'mongodb':
            self.db = MongoDBService(**config)
        else:
            raise ValueError(f"Unknown service type: {service_type}")
    
    def insert(self, data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self.db.insert(data, **kwargs)
    
    def find(self, query: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        return self.db.find(query, **kwargs)
    
    def update(self, query: Dict[str, Any], update_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self.db.update(query, update_data, **kwargs)
    
    def delete(self, query: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self.db.delete(query, **kwargs)
    
    def get_by_id(self, id_value: Any, **kwargs) -> Optional[Dict[str, Any]]:
        return self.db.get_by_id(id_value, **kwargs)
    
    def close(self):
        if hasattr(self.db, 'close'):
            self.db.close()


def get_db_client():

    DB_BACKEND = os.getenv("DB_BACKEND", "mongodb").lower()
    DB_NAME = os.getenv("DB_NAME", None)
    if not DB_NAME:
        raise Exception("DB_NAME not defined")
    print(f"DB_BACKEND: {DB_BACKEND}")
    print(f"DB_NAME: {DB_NAME}")
    
    if DB_BACKEND == "mongodb":
        MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
        return DatabaseWrapper(
            service_type="mongodb",
            database=DB_NAME,
            collection=os.getenv("MONGO_JOBS_COLLECTION", "jobs"),
            connection_string=MONGO_URL,
        )
    elif DB_BACKEND == "dynamodb":
        AWS_REGION = os.getenv("AWS_REGION", "ap-south-2")
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        return DatabaseWrapper(
            service_type="dynamodb",
            table_name=DB_NAME,
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
    else:
        raise Exception("DB_BACKEND not defined")



# Usage:
# dynamo_db = DatabaseWrapper('dynamodb', table_name='users', region_name='ap-south-2')
# mongo_db = DatabaseWrapper('mongodb', database='mydb', collection='users', host='localhost', port=27017)
#
# dynamo_db.insert({'id': '123', 'name': 'John', 'age': 30})
# mongo_db.insert({'name': 'Jane', 'age': 25})
#
# dynamo_users = dynamo_db.find({})
# mongo_users = mongo_db.find({'age': {'$gte': 25}}, limit=10)
#
# dynamo_db.update({'id': '123'}, {'age': 31})
# mongo_db.update({'name': 'Jane'}, {'age': 26})
#
# dynamo_db.delete({'id': '123'})
# mongo_db.delete({'name': 'Jane'})