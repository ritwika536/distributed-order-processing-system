import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello_queue')

def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")

channel.basic_consume(queue='hello_queue', on_message_callback=callback, auto_ack=True)

print("Waiting for messages. Press CTRL+C to stop.")
channel.start_consuming()
