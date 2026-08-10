import pika

# Connect to RabbitMQ running on your computer
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Make sure a queue called 'hello_queue' exists
channel.queue_declare(queue='hello_queue')

# Send a message into that queue
channel.basic_publish(exchange='', routing_key='hello_queue', body='Hello RabbitMQ!')

print("Sent: Hello RabbitMQ!")

connection.close()
