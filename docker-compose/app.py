import json
import paho.mqtt.client as mqtt
import mysql.connector
import os

# MQTT broker configuration
#BROKER = os.getenv('MQTT_BROKER', 'localhost') 
BROKER = '127.0.0.1'
PORT = 1883
TOPIC = '#'  # Use wildcard to capture all Zigbee device messages

def insert_data(topic, payload):
    config = {
    'host': 'localhost',       # Change to your DB host
    'user': 'mqttdata',   # Your MySQL username
    'password': 'mqttdata',  # Your MySQL password
    'database': 'mqtt_data',
    'port': 3406   # The name of your database
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
client = mqtt.Client()
client.on_message = on_message

client.connect(BROKER, PORT, 60)
client.subscribe(TOPIC)

print(f"Subscribed to topic: {TOPIC}")
client.loop_forever()
