

openapi: 3.0.0
info:
  title: Metadata Service API
  version: 1.0.0
  description: API for managing metadata with CRUD operations.
servers:
  - url: http://localhost:5000
paths:
  /metadata:
    post:
      summary: Create metadata
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Metadata'
      responses:
        '201':
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Metadata'
  /metadata/{bri}:
    get:
      summary: Retrieve metadata by BRI
      parameters:
        - name: bri
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Metadata'
        '404':
          description: Not Found
    put:
      summary: Update metadata by BRI
      parameters:
        - name: bri
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Metadata'
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Metadata'
        '404':
          description: Not Found
    delete:
      summary: Delete metadata by BRI
      parameters:
        - name: bri
          in: path
          required: true
          schema:
            type: string
      responses:
        '204':
          description: No Content
        '404':
          description: Not Found
  /metadata/{bri}/history:
    get:
      summary: Retrieve version history of metadata by BRI
      parameters:
        - name: bri
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Metadata'
        '404':
          description: Not Found
components:
  schemas:
    Metadata:
      type: object
      properties:
        metadatatype:
          type: string
          description: Type of the metadata.
        businessDescription:
          type: string
          description: Business description of the metadata.
        businessTitle:
          type: string
          description: Business title for the metadata.
        metadataclass:
          type: string
          description: Class of the metadata.
        datatype:
          type: string
          description: Data type of the metadata.
        bri:
          type: string
          description: Unique identifier for the metadata.
        name:
          type: string
          description: Name of the metadata.
        nullable:
          type: boolean
          description: Indicates if the metadata can be null.
        primaryKeyColumn:
          type: boolean
          description: Indicates if this metadata is a primary key.
        retention:
          type: integer
          description: Retention period for the metadata.
        tag:
          type: array
          items:
            type: string
          description: Array of tags associated with the metadata.
        additional_metadata:
          type: object
          description: Additional metadata as a JSON object.
      required:
        - metadatatype
        - datatype
        - bri
        - name
        
        
        
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_swagger_ui import get_swaggerui_blueprint
from marshmallow import Schema, fields, ValidationError
from redis_cache import RedisCache
from models import db, Metadata
from config import Config

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

# Initialize Redis Cache
cache = RedisCache()

# Swagger UI setup
SWAGGER_URL = '/swagger'
API_URL = '/openapi.yaml'  # URL for OpenAPI specification
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "Metadata Service API"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

class MetadataSchema(Schema):
    metadatatype = fields.Str(required=True, default='Metadata')
    businessDescription = fields.Str()
    businessTitle = fields.Str()
    metadataclass = fields.Str(required=True, default='relational')
    datatype = fields.Str(required=True)
    bri = fields.Str(required=True)
    name = fields.Str(required=True)
    nullable = fields.Bool(default=False)
    primaryKeyColumn = fields.Bool(default=False)
    retention = fields.Int(default=365)
    tag = fields.List(fields.Str())  # Array of text for tags
    additional_metadata = fields.Dict()  # JSON field for additional metadata

metadata_schema = MetadataSchema()

@app.route('/metadata', methods=['POST'])
def create_metadata():
    """Create metadata"""
    try:
        data = metadata_schema.load(request.json)
        new_metadata = Metadata(**data)
        db.session.add(new_metadata)
        db.session.commit()
        cache.set(new_metadata.bri, new_metadata.to_dict(), timeout=14400)  # Cache for 4 hours
        return jsonify(new_metadata.to_dict()), 201
    except ValidationError as err:
        return jsonify(err.messages), 400

@app.route('/metadata/<bri>', methods=['GET'])
def get_metadata(bri):
    """Retrieve metadata by BRI"""
    cached_data = cache.get(bri)
    if cached_data:
        return jsonify(cached_data), 200

    metadata = Metadata.query.filter_by(bri=bri).first()
    if not metadata:
        return jsonify({"error": "Metadata not found"}), 404

    cache.set(bri, metadata.to_dict(), timeout=14400)  # Cache for 4 hours
    return jsonify(metadata.to_dict()), 200

@app.route('/metadata/<bri>', methods=['PUT'])
def update_metadata(bri):
    """Update metadata by BRI"""
    try:
        data = metadata_schema.load(request.json)
        metadata = Metadata.query.filter_by(bri=bri).first()
        if not metadata:
            return jsonify({"error": "Metadata not found"}), 404

        for key, value in data.items():
            setattr(metadata, key, value)
        db.session.commit()
        cache.set(bri, metadata.to_dict(), timeout=14400)  # Update cache
        return jsonify(metadata.to_dict()), 200
    except ValidationError as err:
        return jsonify(err.messages), 400

@app.route('/metadata/<bri>', methods=['DELETE'])
def delete_metadata(bri):
    """Delete metadata by BRI"""
    metadata = Metadata.query.filter_by(bri=bri).first()
    if not metadata:
        return jsonify({"error": "Metadata not found"}), 404

    db.session.delete(metadata)
    db.session.commit()
    cache.delete(bri)  # Remove from cache
    return jsonify({"message": "Deleted successfully"}), 204

@app.route('/metadata/<bri>/history', methods=['GET'])
def get_metadata_history(bri):
    """Retrieve version history of metadata by BRI"""
    metadata = Metadata.query.filter_by(bri=bri).first()
    if not metadata:
        return jsonify({"error": "Metadata not found"}), 404

    history = metadata.versions  # Access the history of changes
    return jsonify([version.to_dict() for version in history]), 200

@app.route('/openapi.yaml', methods=['GET'])
def openapi_spec():
    """Serve OpenAPI specification"""
    with open('swagger.yaml') as f:
        return f.read(), 200

if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create tables
    app.run(debug=True)
    
    
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Metadata(db.Model):
    __tablename__ = 'metadata'
    id = db.Column(db.Integer, primary_key=True)
    metadatatype = db.Column(db.String(50), nullable=False, default='Metadata')
    businessDescription = db.Column(db.Text)
    businessTitle = db.Column(db.String(100))
    metadataclass = db.Column(db.String(50), nullable=False, default='relational')
    datatype = db.Column(db.String(50), nullable=False)
    bri = db.Column(db.String(100), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    nullable = db.Column(db.Boolean, default=False)
    primaryKeyColumn = db.Column(db.Boolean, default=False)
    retention = db.Column(db.Integer, default=365)
    tag = db.Column(db.ARRAY(db.String))  # Array of text for tags
    additional_metadata = db.Column(db.JSON)  # JSON field for additional metadata

    def to_dict(self):
        return {
            "metadatatype": self.metadatatype,
            "businessDescription": self.businessDescription,
            "businessTitle": self.businessTitle,
            "metadataclass": self.metadataclass,
            "datatype": self.datatype,
            "bri": self.bri,
            "name": self.name,
            "nullable": self.nullable,
            "primaryKeyColumn": self.primaryKeyColumn,
            "retention": self.retention,
            "tag": self.tag,
            "additional_metadata": self.additional_metadata
        }
        


import redis

class RedisCache:
    def __init__(self, host='localhost', port=6379):
        self.client = redis.StrictRedis(host=host, port=port, db=0)

    def get(self, key):
        value = self.client.get(key)
        return value if value is None else value.decode('utf-8')

    def set(self, key, value, timeout):
        self.client.set(key, value, ex=timeout)

    def delete(self, key):
        self.client.delete(key)
        


import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import db, Metadata  # Make sure to import your models

class MetadataConsumer:
    def __init__(self, kafka_server, topic, db_url):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def process_event(self, event):
        action = event.get('action')
        if action == 'create':
            self.create_metadata(event['metadata'])
        elif action == 'update':
            self.update_metadata(event['metadata'])
        elif action == 'delete':
            self.delete_metadata(event['bri'])

    def create_metadata(self, metadata):
        session = self.Session()
        new_metadata = Metadata(**metadata)
        session.add(new_metadata)
        session.commit()
        session.close()
        print(f"Created metadata: {new_metadata.bri}")

    def update_metadata(self, metadata):
        session = self.Session()
        existing_metadata = session.query(Metadata).filter_by(bri=metadata['bri']).first()
        if existing_metadata:
            for key, value in metadata.items():
                setattr(existing_metadata, key, value)
            session.commit()
            print(f"Updated metadata: {existing_metadata.bri}")
        else:
            print(f"Metadata not found for update: {metadata['bri']}")
        session.close()

    def delete_metadata(self, bri):
        session = self.Session()
        existing_metadata = session.query(Metadata).filter_by(bri=bri).first()
        if existing_metadata:
            session.delete(existing_metadata)
            session.commit()
            print(f"Deleted metadata: {bri}")
        else:
            print(f"Metadata not found for delete: {bri}")
        session.close()

    def run(self):
        for message in self.consumer:
            self.process_event(message.value)

if __name__ == "__main__":
    kafka_server = 'localhost:9092'
    topic = 'metadata_events'
    db_url = 'postgresql://username:password@localhost:5432/metadata_db'  # Update with your DB credentials

    consumer = MetadataConsumer(kafka_server, topic, db_url)
    consumer.run()
    

import json
import boto3
import psycopg2
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3FileConsumer:
    def __init__(self, bucket_name, db_url, aws_access_key, aws_secret_key, region):
        self.bucket_name = bucket_name
        self.db_url = db_url
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

    def fetch_files(self):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' not in response:
                print("No files found in the bucket.")
                return

            for obj in response['Contents']:
                file_key = obj['Key']
                self.process_file(file_key)

        except (NoCredentialsError, PartialCredentialsError):
            print("Credentials not available.")
        except Exception as e:
            print(f"Error fetching files: {e}")

    def process_file(self, file_key):
        try:
            print(f"Processing file: {file_key}")
            file_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            data = json.loads(file_content['Body'].read().decode('utf-8'))

            # Load the data into PostgreSQL
            self.load_to_postgres(data)

        except Exception as e:
            print(f"Error processing file {file_key}: {e}")

    def load_to_postgres(self, metadata):
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO Metadata (metadatatype, businessDescription, businessTitle, metadataclass, datatype, bri, name, nullable, primaryKeyColumn, retention, tag, additional_metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                metadata['metadatatype'],
                metadata.get('businessDescription'),
                metadata.get('businessTitle'),
                metadata['metadataclass'],
                metadata['datatype'],
                metadata['bri'],
                metadata['name'],
                metadata.get('nullable', False),
                metadata.get('primaryKeyColumn', False),
                metadata.get('retention', 365),
                metadata.get('tag', []),
                metadata.get('additional_metadata', {})
            ))
            conn.commit()
            print(f"Loaded metadata: {metadata['bri']}")
        except Exception as e:
            print(f"Error loading metadata to PostgreSQL: {e}")
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    bucket_name = 'your-s3-bucket-name'  # Replace with your S3 bucket name
    db_url = 'postgresql://username:password@localhost:5432/metadata_db'  # Update with your DB credentials
    aws_access_key = 'your_aws_access_key'  # Replace with your AWS Access Key
    aws_secret_key = 'your_aws_secret_key'  # Replace with your AWS Secret Key
    region = 'your_region'  # e.g., 'us-west-2'

    s3_consumer = S3FileConsumer(bucket_name, db_url, aws_access_key, aws_secret_key, region)
    s3_consumer.fetch_files()
    


CREATE TABLE Metadata (
    id SERIAL PRIMARY KEY,
    metadatatype VARCHAR(50) NOT NULL DEFAULT 'Metadata',
    businessDescription TEXT,
    businessTitle VARCHAR(100),
    metadataclass VARCHAR(50) NOT NULL DEFAULT 'relational',
    datatype VARCHAR(50) NOT NULL,
    bri VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    nullable BOOLEAN DEFAULT FALSE,
    primaryKeyColumn BOOLEAN DEFAULT FALSE,
    retention INTEGER DEFAULT 365,
    tag TEXT[],  -- Array of text for tags
    additional_metadata JSONB,  -- JSON field for additional metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Optional: Track creation time
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Optional: Track last update time
    version_id INTEGER,  -- Optional: Version ID for tracking, if necessary
    CONSTRAINT fk_version FOREIGN KEY (version_id) REFERENCES Metadata_version(id)  -- Foreign key to version table
);

CREATE INDEX idx_bri ON Metadata (bri);



