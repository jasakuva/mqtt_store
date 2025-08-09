import json
import mysql.connector
import os

def createDatabase():
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

        query = """CREATE TABLE `message_json` (
                `messageid` int NOT NULL AUTO_INCREMENT,
                `sourceid` int DEFAULT NULL,
                `message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                `topic` varchar(255) NOT NULL,
                `crationtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`messageid`),
                UNIQUE KEY `messageid` (`messageid`),
                KEY `sourceid` (`sourceid`)
                ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
        
        cursor.execute(query)


        query = """CREATE TABLE `message_variabledata` (
                   `messageid` int NOT NULL,
                   `variable` varchar(255) NOT NULL,
                   `data` varchar(255) NOT NULL
                   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""

        cursor.execute(query)


        query = """CREATE TABLE `sources` (
                   `sourceid` int NOT NULL AUTO_INCREMENT,
                   `name` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
                   `source_mqtt_key` varchar(255) NOT NULL,
                   PRIMARY KEY (`sourceid`),
                   KEY `sourceid` (`sourceid`)
                   ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""

        cursor.execute(query)

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()


