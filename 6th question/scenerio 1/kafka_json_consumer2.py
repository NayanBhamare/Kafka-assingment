import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


API_KEY = 'FORCVNK7NJ55O2HM'
ENDPOINT_SCHEMA_URL  = 'https://psrc-e0919.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'l60Ypf5ZXDkbxNZ03GpXN9M7GaRxZr1qW3NBMi285t/QDuJ7U8Ww2T80KzH2/IwU'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'ZCYNFUE6JMZEK6AZ'
SCHEMA_REGISTRY_API_SECRET = 'U60QXOtMErz5gbEwabsBmFbW38cjGOo1tWnEQkFh6BqXOzZqydR+oSBa0B/kDSEX' 


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"



def main(topic):
# info -->******************  hardcoded schema ******************

#     schema_str = """
# {
#   "$id": "http://example.com/myURI.schema.json",
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "additionalProperties": false,
#   "description": "Sample schema to help you get started.",
#   "properties": {  					
#     "Order Number": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Order Date": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Item Name": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Quantity": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Product Price": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Total products": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     }
#   },
#   "title": "SampleRecord",
#   "type": "object"
# }
#     """

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Info --> ****************** approch 1 to get schema from schema registry using subject ******************
    # subjects = schema_registry_client.get_subjects()
    # for sub in subjects:
    #     if sub == 'restaurant-take-away-data-value':
    #         schema = schema_registry_client.get_latest_version(sub)
    #         schema_str = schema.schema.schema_str

# Info --> ****************** approch 2 to get schema from schema registry using schema id ******************

    schema_str = schema_registry_client.get_schema(schema_id = 100005).schema_str       
    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group2',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), restaurant))
                counter+= 1
            print(f'totoal number of recorded subscribed are : {counter}')

        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurant-take-away-data")