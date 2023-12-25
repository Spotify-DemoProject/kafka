import os, sys

config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config/config.ini")
lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../lib")
sys.path.append(lib_dir)

from kafka_libs import *

def create_topics():
    from configparser import ConfigParser
    
    config = ConfigParser()
    config.read(config_dir)
    broker = config.get("KAFKA", "broker")
    
    create_topic(broker=broker, name="spotify-raw")
    create_topic(broker=broker, name="spotify-record")


if __name__ == "__main__":
    create_topics()
