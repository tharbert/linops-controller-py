#!/usr/bin/python

from kafka import KafkaConsumer
from slacker import Slacker


KAFKA_SERVER = 'x.x.x.x:y'
SLACK_API_KEY = 'x'
SLACK_CHANNEL = '#x'


def post_to_slack(slack_message):
    slack = Slacker(SLACK_API_KEY)
    slack.chat.post_message(SLACK_CHANNEL, slack_message)


def main():
    # consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('quagga-bgp',
                             group_id='linops-controller.py',
                             bootstrap_servers=[KAFKA_SERVER])
    for message in consumer:
        post_to_slack(str(message.value))


if __name__ == '__main__':
    main()
