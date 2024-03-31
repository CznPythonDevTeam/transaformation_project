from confluent_kafka import Producer, Consumer
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
import pandas as pd
from confluent_kafka import Consumer, KafkaError

class snowfalke_db_manager():

  def __init__(self):  
    # Create cursor
    self.sf_conn, self.sf_cur = self.get_sf_connection()

  def get_sf_connection(self):
    '''
    Read the Snowflake configuration and save them in the environment variables. For Example,
    the Snowfalke account will be in environment variable SF_ACCOUNT.
    '''

    # Read the Snowflake configuration
    load_dotenv("./.virsec_sf_config.env")

    # Creating Snowflake connection with the user account deatils
    sf_conn = snow.connect(
      account=os.environ.get('SF_ACCOUNT'),
      user=os.environ.get('SF_USER'),
      password=os.environ.get('SF_PWD'),
      warehouse=os.environ.get('SF_WAREHOUSE'),
      role=os.environ.get('SF_ROLE'),
      database=os.environ.get('SF_DATABASE'),
      schema=os.environ.get('SF_SCHEMA')
    )

    sf_cur = sf_conn.cursor()
    return sf_conn, sf_cur

  def push_snowflake_db(self,dataframe):  
    print(dataframe,"dataframe")
    success, nchunks, nrows, _ = write_pandas(self.sf_conn,
                                                      df=dataframe,
                                                      table_name="murali_test_systemintegrity",
                                                      auto_create_table=True,
                                              schema="DEV_SCHEMA")


class kafka_consumer_data():
  
  def __init__(self):
    self.conf = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'abc',        # Consumer group ID
        'auto.offset.reset': 'earliest'         # Start consuming from the earliest message
    }

    self.topic_names = ['test_topic2']

  def get_json_data(self):
    # Create Kafka consumer
    consumer = Consumer(self.conf)
    # Subscribe to Kafka topic
    consumer.subscribe(self.topic_names)
    
    # Poll for new messages
    accumulated_data = []
    try:
        while True:
            msg = consumer.poll(2.0)  # Poll for new messages, with a timeout of 2 second
            print(msg)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, the consumer reached the end of the topic
                    continue
                else:
                    # Other error
                    print(msg.error())
                    break
            # Print the consumed message
            msg_value = msg.value().decode('utf-8')
            print('Received message: {}'.format(msg_value))
            accumulated_data.append(msg_value)
            if len(accumulated_data)==3:
                break
            # return msg_value
    except KeyboardInterrupt:
        # Stop consuming when Ctrl+C is pressed
        consumer.close()
    return accumulated_data

'''
app_instance_data={
  "_id": {
    "_id": {
      "$oid": "64fab906ed6a053bae78230c"
    }
  },
  "documentKey": {
    "_id": {
      "$oid": "64fab906ed6a053bae78230c"
    }
  },
  "fullDocument": {
    "_class": "com.virsec.cms.domain.n.b",
    "_id": {
      "$oid": "64fab906ed6a053bae78230c"
    },
    "aiStatus": "UNKNOWN",
    "aliveSinceTime": {
      "$numberLong": "0"
    },
    "applicationsCount": {
      "$numberLong": "0"
    },
    "auditFields": {
      "createdBy": "system",
      "createdOn": {
        "$numberLong": "1694152966816"
      },
      "modifiedBy": "system",
      "modifiedOn": {
        "$numberLong": "1694778687258"
      }
    },
    "canary": {
      "$numberLong": "1045380710"
    },
    "deleted": True,
    "firstInstalledTime": {
      "$numberLong": "1692801185000"
    },
    "installedVersion": "2.10.0.20230819135807",
    "instanceTypes": [
      "APPLICATIONSERVER"
    ],
    "lastDisconnectionTime": {
      "$numberLong": "1694778687258"
    },
    "lastInstalledTime": {
      "$numberLong": "1692801185000"
    },
    "name": "EXSERVER2019",
    "operatingSystem": {
      "name": "Windows",
      "nameId": "64fab8bb81a8d21f6acbcdfb",
      "version": "2019",
      "versionId": "64fab906ed6a053bae78230b"
    },
    "org_id": "default",
    "rmpSupported": True,
    "root_id": {
      "$numberLong": "1"
    },
    "serverNetworkInfo": {
      "ipAddress": "10.16.30.105"
    },
    "serviceInstanceAlias": "EXSERVER2019",
    "serviceInstanceId": "10.16.30.105",
    "serviceInstanceType": "ASI"
  },
  "ns": {
    "coll": "application_instance",
    "db": "zeus"
  },
  "operationType": "insert",
  "tenantId": "BOA"
}
ds_installer_data={
  "_id": {
    "_id": {
      "$oid": "65bb89462dad593ec04704c5"
    }
  },
  "documentKey": {
    "_id": {
      "$oid": "65bb89462dad593ec04704c5"
    }
  },
  "fullDocument": {
    "_class": "com.virsec.cms.domain.ProfileBasedInstaller",
    "_id": {
      "$oid": "65bb89462dad593ec04704c5"
    },
    "auditFields": {
      "createdBy": "system",
      "createdOn": {
        "$numberLong": "1706789190978"
      }
    },
    "edited": False,
    "expiry": "",
    "profileId": {
      "$numberLong": "422244579"
    },
    "publisher": "Oren Novotny",
    "selected": True,
    "source": "SCAN"
  },
  "ns": {
    "coll": "ds_installer",
    "db": "zeus"
  },
  "operationType": "insert",
  "tenantId": "BOA"
}
process_details_data={
  "_id": {
    "_id": {
      "$oid": "65dd887761b05243f1d21c91"
    }
  },
  "documentKey": {
    "_id": {
      "$oid": "65dd887761b05243f1d21c91"
    }
  },
  "fullDocument": {
    "_class": "com.virsec.cms.domain.ProcessDetail",
    "_id": {
      "$oid": "65dd887761b05243f1d21c91"
    },
    "checksum": "cc3d8f6063bf3d59c0b671ed23f63484",
    "commandline": "",
    "edited": False,
    "firstSeenHostOId": "65dd870d4348a018ddad5cb6",
    "firstSeenHostname": "Rhel7-Java",
    "foundInScan": True,
    "globalSelected": False,
    "key": "5e5f11c46beaf693ab0d59f8a7392332",
    "lastSeen": {
      "$numberLong": "1709017207366"
    },
    "libMonitorEnabled": True,
    "parentProcess": {
      "selected": False
    },
    "path": "/var/lib/docker/overlay2/b9dfba43281d0ea8471464020f571e5287121c88cf2eee28f9f09949cf6b6096/diff/opt/java/openjdk/bin/jdeps",
    "pid": {
      "$numberLong": "0"
    },
    "processname": "jdeps",
    "profileId": {
      "$numberLong": "42096738"
    },
    "profileLibMonitorEnabled": True,
    "profileName": "Linux-Profile",
    "profileObjectId": "65dd8864659bd307ab54a0fb",
    "selected": False,
    "selectedBySystem": False,
    "source": "SCAN",
    "threatVerificationDetail": {
      "message": "Low Trust Sources",
      "status": "UNKNOWN",
      "threatScore": 0,
      "verificationAttempts": 0
    },
    "username": ""
  },
  "ns": {
    "coll": "process_detail",
    "db": "zeus"
  },
  "operationType": "insert",
  "tenantId": "BOA"
}
'''

# df=pd.DataFrame({
#     "TenantID":["BOA"],
#     "Telemetry source":["workload"],
#     "Telemetry type":["workload"],
#     "Telemetry version":["v1"],
#     "workload_id":["12.12.12.12"],
#     "IP address":["10.16.30.105"],
#     "hostname":["rhel7"],
#     "CPUID":[""],
#     "OS type":["RHEL"],
#     "Kernel version":[""],
#     "timestamp":[""]
#     })

def json_data_transformation(app_instance_data,ds_installer_data,process_details_data):

  df=pd.DataFrame()

  ## added code ##
  import json
  json_file_path = 'SystemIntegrity/system_integrity_schema.json'
  with open(json_file_path, 'r') as json_file:
      data_dict = json.load(json_file)

  APPLICATION_INSTANCE = pd.json_normalize(app_instance_data)
  ds_installer = pd.json_normalize(ds_installer_data)
  process_detail = pd.json_normalize(process_details_data)

  hardcoded_dict = {'APPLICATION_INSTANCE':APPLICATION_INSTANCE,'ds_installer':ds_installer,'process_detail':process_detail}

  row_data = {}
  for col_key, col_data in data_dict.items():
      if col_data:
          source_df = col_data['source']
          source_df_name = hardcoded_dict[source_df]
          source_key = col_data['key']
          column_name = next(filter(lambda col: source_key in col, source_df_name.columns), None)
          if column_name:
              val = source_df_name[column_name].values
              row_data[col_key] = val
          else:
              row_data[col_key] = ''
      else:
          row_data[col_key] = ''

  output_df = pd.DataFrame(row_data)
  return output_df


kafka_consumer = kafka_consumer_data()
consumer_json_data = kafka_consumer.get_json_data()

app_instance_data = eval(consumer_json_data[0])
ds_installer_data = eval(consumer_json_data[1])
process_details_data = eval(consumer_json_data[2])

output_df = json_data_transformation(app_instance_data,ds_installer_data,process_details_data)
print(output_df,"output_df")

sf_db = snowfalke_db_manager()
sf_db.push_snowflake_db(output_df)

# p = Producer({'bootstrap.servers': 'localhost:9092'})
# p.produce('mytopic', 'my message')
# c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup'})
# c.subscribe(['mytopic'])
# msg = c.poll(1.0)
# print(msg.value())
