import json
import paho.mqtt.client as mqtt
import mysql.connector
import os
import time
import createDatabase

# MQTT broker configuration
BROKER = os.getenv('MQTT_BROKER', 'localhost') 
PORT = os.getenv('MQTT_PORT', 1883)
TOPIC =  os.getenv('MQTT_TOPIC', '#')

def insert_data(topic, payload):
    config = {
    'host':  os.getenv('MYSQL_HOST', 'localhost'),
    'user':  os.getenv('MYSQL_USER', 'mqttdata'),
    'password': os.getenv('MYSQL_PWD', 'mqttdata'),
    'database': os.getenv('MYSQL_DATABASE', 'mqtt_data'),
    'port': os.getenv('MYSQL_PORT', 3306)
    }

    try:
        # Establish connection
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)
        #cursor = conn.cursor()
        
        query = "SELECT * FROM sources WHERE source_mqtt_key = %s"
        cursor.execute(query, (topic,))  # ← Use a tuple even for one value

        row = cursor.fetchone()

        if row:
            source_id = row['sourceid']
        else:
            query = "INSERT INTO sources (source_mqtt_key) VALUES (%s)"
            values = topic,

            # Execute and commit
            cursor.execute(query, values)
            source_id = cursor.lastrowid

        # Insert query (skip messageid)
        query = "INSERT INTO message_json (message, topic, sourceid) VALUES (%s, %s, %s)"
        values = (payload, topic, source_id)

        # Execute and commit
        cursor.execute(query, values)
        last_id = cursor.lastrowid
        
        data = json.loads(payload)

        if isinstance(data, dict):
            for key, value in data.items():
                print(f"{key}: {value}")
                query = "INSERT INTO message_variabledata (messageid, variable, data) VALUES (%s, %s, %s)"
                
                if isinstance(value, dict):
                    value_str = json.dumps(value)
                else:
                    value_str = str(value)
                
                values = (last_id, key, value_str)

                # Execute and commit
                cursor.execute(query, values)
        else:
            print("Payload is not a dictionary:", data)
        
        conn.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and conn.is_connected():
            conn.close()

# Callback when a message is received
def on_message(client, userdata, msg):
    print(f"\nMessage received on topic: {msg.topic}")
    insert_data(msg.topic, msg.payload.decode('utf-8'))
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)

        if isinstance(data, dict):
            for key, value in data.items():
                print(f"{key}: {value}")
        else:
            print("Payload is not a dictionary:", data)

    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Setup MQTT client
file_path = "/tmp/mqtt_mysql_status.txt"

try:
    # Try to open and read the first line
    with open(file_path, 'r') as file:
        first_line = file.readline().strip()
        print(f"First line: {first_line}")

except FileNotFoundError:
    # File doesn't exist — do your fallback actions here

    time.sleep(60)
    createDatabase.createDatabase()
    print("File not found. Performing fallback actions...")

    # Example: create the file and write something into it
    with open(file_path, 'w') as file:
        file.write("This is the first line\n")
    print(f"File '{file_path}' created.")

client = mqtt.Client()
client.on_message = on_message

client.connect(BROKER, PORT, 60)
client.subscribe(TOPIC)

print(f"Subscribed to topic: {TOPIC}")
client.loop_forever()
