import rospy
import json
import numpy as np
import time

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from sensor_msgs.msg import Image
from rospy_message_converter import message_converter, json_message_converter

class VideoStreamer:
    def __init__(self):

        # Initialize the node
        rospy.init_node('video_streamer', anonymous=True)

        # Kafka Admin Client
        self.admin_client = KafkaAdminClient(bootstrap_servers='192.168.50.2:29094', client_id='test_admin_client')

        # Kafka producer
        self.producer = KafkaProducer(bootstrap_servers='192.168.50.2:29094', acks=0, batch_size=10, compression_type='gzip', max_request_size=5000000) #, value_serializer=lambda m: json.dumps(m).encode('ascii'))

        # Create the topic if it does not exist
        topic = 'test_video'
        topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        if topic not in self.admin_client.list_topics():
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

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

        start_time = time.time()
        msg_bytes = msg.data
        self.producer.send('test_video', msg_bytes)
        end_time = time.time()
        
        print("Time to send: ", end_time - start_time)

        # msg_dict = message_converter.convert_ros_message_to_dictionary(msg)
        # print("Sending Message")
        # self.producer.send('test_video', msg_dict)

    def run(self):
        rospy.spin()

if __name__ == '__main__':
    streamer = VideoStreamer()
    streamer.run()