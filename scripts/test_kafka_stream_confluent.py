import rospy
import json
import numpy as np
import time
import struct

from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from sensor_msgs.msg import Image
from rospy_message_converter import message_converter, json_message_converter

def serialize_key(key):
    """
    Serializes an integer key to a bytes object.
    """
    return struct.pack('>i', key) if key is not None else None

class VideoStreamer:
    def __init__(self):

        # Initialize the node
        rospy.init_node('video_streamer', anonymous=True)

        conf = {'bootstrap.servers': '192.168.50.2:29094',
            'client.id': 'test_producer',
            "compression.type": "gzip",
            "queue.buffering.max.messages": 0}

        # Kafka Admin Client
        self.admin_client = AdminClient(conf)

        # Kafka producer
        self.producer = Producer(conf)
    
        # Create the topic if it does not exist
        self.topic = "test_video"
        new_topic = NewTopic(self.topic, num_partitions=1, replication_factor=1)
        fs = self.admin_client.create_topics([new_topic])

        self.msg_key = serialize_key(rospy.get_param('~robot_id', 0))

        self.status = True
        self.index = 0

         # Subscribe to the ros video topic
        self.subscriber = rospy.Subscriber('/camera/color/image_raw', Image, self.callback)

    def callback(self, msg):
        # msg_dict = dict()
        # image_list = np.array(msg.data).tolist()
        # msg_dict['data'] = image_list
        # self.producer.send('test_video', msg_dict)

        # print(msg.data)

        # Send the video frame to Kafka
        # json_str = json_message_converter.convert_ros_message_to_json(msg)
        # print(json_str)

        if self.index == 0:
            msg_bytes = msg.data

            # Send the message, if get error handle it
            try:
                self.producer.produce(self.topic, value=msg_bytes, key=self.msg_key)
            except KafkaException as e:
                print(e)
            except BufferError as e:
                print("Buffer error")
                self.producer.poll(0)
            except KafkaError as e:
                print(e)
        
        self.index += 1
        if self.index == 4:
            self.index = 0

    def run(self):
        rospy.spin()

if __name__ == '__main__':
    streamer = VideoStreamer()
    streamer.run()