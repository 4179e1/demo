#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

# If message cannot be routed to a queue, we need confirm_delivery() to get noticed. 
# see https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish
#     https://pika.readthedocs.io/en/latest/examples/blocking_publish_mandatory.html
channel.confirm_delivery()


severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
message = ' '.join(sys.argv[2:]) or 'Hello World!'

try:
    res = channel.basic_publish(
        exchange='direct_logs', 
        routing_key=severity, 
        mandatory=True, 
        body=message
    )
    print(" [x] Sent %r:%r" % (severity, message))
except  pika.exceptions.UnroutableError:
    print("Message was returned")
connection.close()