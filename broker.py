import socket
import threading
import json
import sys
import pickle
import os

# Persistența datelor
data_file = 'broker_data.pkl'
if os.path.exists(data_file):
    with open(data_file, 'rb') as f:
        broker_data = pickle.load(f)
else:
    broker_data = {
        'topics': {},  # {'topic_name': [message1, message2, ...]}
        'subscribers': {},  # {'subscriber_name': {'topics': [topic1, topic2], 'address': (ip, port)}}
        'publishers': {},  # {'publisher_name': {'topics': [topic1, topic2]}}
        'offline_messages': {}  # {'subscriber_name': [message_dict, ...]}
    }

protocol = input("Alege protocolul de transport pentru broker (TCP/UDP): ").upper()
if protocol not in ['TCP', 'UDP']:
    print("Protocol invalid. Alege TCP sau UDP.")
    sys.exit()

# Adresa și portul broker-ului
HOST = 'localhost'
PORT = 10000


def save_data():
    with open(data_file, 'wb') as f:
        pickle.dump(broker_data, f)


def handle_connection(conn, addr, protocol):
    try:
        data = conn.recv(4096)
        process_message(data, addr, protocol)
    except Exception as e:
        print(f"Eroare la gestionarea conexiunii: {e}")
    finally:
        conn.close()


def process_message(data, addr, protocol):
    try:
        message = json.loads(data.decode('utf-8'))
        action = message.get('action')
        if action == 'publisher_connect':
            handle_publisher_connect(message)
        elif action == 'create_topic':
            handle_create_topic(message)
        elif action == 'publish':
            handle_publication(message)
        elif action == 'subscriber_connect':
            handle_subscriber_connect(message)
        elif action == 'subscribe':
            handle_subscription(message)
        else:
            print("Acțiune necunoscută.")
    except Exception as e:
        print(f"Eroare la procesarea mesajului: {e}")


def handle_publisher_connect(message):
    publisher = message['publisher']

    if publisher not in broker_data['publishers']:
        broker_data['publishers'][publisher] = {'topics': []}
        print(f"Publisher nou conectat: {publisher}")

    # Trimiterea topicurilor existente către publisher
    response = {
        'action': 'publisher_topics',
        'topics': broker_data['publishers'][publisher]['topics']
    }
    send_response(message['protocol'], message['address'], response)
    save_data()


def handle_create_topic(message):
    publisher = message['publisher']
    topic = message['topic']

    if topic not in broker_data['topics']:
        broker_data['topics'][topic] = []
        broker_data['publishers'][publisher]['topics'].append(topic)
        print(f"Publisher '{publisher}' a creat topicul '{topic}'.")
    else:
        print(f"Topicul '{topic}' există deja.")

    save_data()


def handle_publication(message):
    publisher = message['publisher']
    topic = message['topic']
    msg_content = message['message']

    if topic not in broker_data['topics']:
        print(f"Topicul '{topic}' nu există. Creare automată.")
        broker_data['topics'][topic] = []
        broker_data['publishers'][publisher]['topics'].append(topic)

    broker_data['topics'][topic].append(msg_content)
    print(f"Mesaj primit de la '{publisher}' pe topicul '{topic}': {msg_content}")
    route_message(topic, msg_content)
    save_data()


def handle_subscriber_connect(message):
    subscriber = message['subscriber']

    if subscriber not in broker_data['subscribers']:
        broker_data['subscribers'][subscriber] = {'topics': [], 'address': message['address']}
        print(f"Subscriber nou conectat: {subscriber}")
    else:
        # Actualizăm adresa subscriber-ului
        broker_data['subscribers'][subscriber]['address'] = message['address']
        print(f"Subscriber '{subscriber}' s-a reconectat.")

    # Trimiterea topicurilor abonate și a mesajelor offline
    response = {
        'action': 'subscriber_data',
        'topics': broker_data['subscribers'][subscriber]['topics'],
        'offline_messages': broker_data['offline_messages'].get(subscriber, [])
    }
    send_response(message['protocol'], message['address'], response)

    # Ștergem mesajele offline după livrare
    if subscriber in broker_data['offline_messages']:
        del broker_data['offline_messages'][subscriber]
    save_data()


def handle_subscription(message):
    subscriber = message['subscriber']
    topic = message['topic']

    if subscriber not in broker_data['subscribers']:
        broker_data['subscribers'][subscriber] = {'topics': [topic], 'address': message['address']}
    else:
        if topic not in broker_data['subscribers'][subscriber]['topics']:
            broker_data['subscribers'][subscriber]['topics'].append(topic)

    print(f"Subscriber '{subscriber}' s-a abonat la '{topic}'.")
    save_data()


def route_message(topic, message):
    for subscriber, data in broker_data['subscribers'].items():
        if topic in data['topics']:
            try:
                send_to_subscriber(subscriber, data['address'], {'topic': topic, 'message': message})
            except Exception as e:
                print(f"Eroare la trimiterea către '{subscriber}': {e}")

                if subscriber not in broker_data['offline_messages']:
                    broker_data['offline_messages'][subscriber] = []

                broker_data['offline_messages'][subscriber].append({'topic': topic, 'message': message})
                save_data()


def send_to_subscriber(subscriber, address, message):
    serialized_data = json.dumps(message).encode('utf-8')
    try:
        if protocol == 'TCP':
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(tuple(address))
                sock.sendall(serialized_data)
                print(f"Mesaj trimis către '{subscriber}'.")
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(serialized_data, tuple(address))
                print(f"Mesaj trimis către '{subscriber}'.")

    except Exception as e:
        print(f"Eroare la trimiterea mesajului către '{subscriber}' ({address}): {e}")
        # Salvăm mesajul ca fiind offline
        if subscriber not in broker_data['offline_messages']:
            broker_data['offline_messages'][subscriber] = []
        broker_data['offline_messages'][subscriber].append(message)
        save_data()  # Salvăm datele offline


def send_response(protocol, address, response):
    serialized_data = json.dumps(response).encode('utf-8')
    if protocol == 'TCP':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(tuple(address))
                sock.sendall(serialized_data)
            except Exception as e:
                print(f"Eroare la trimiterea răspunsului către {address}: {e}")
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.sendto(serialized_data, tuple(address))
            except Exception as e:
                print(f"Eroare la trimiterea răspunsului către {address}: {e}")


def main():
    if protocol == 'TCP':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            try:
                server_sock.bind((HOST, PORT))
            except OSError as e:
                print(f"Eroare la legarea socket-ului broker-ului: {e}")
                sys.exit(1)

            server_sock.listen()
            print(f"Broker-ul rulează pe {HOST}:{PORT} folosind TCP.")

            while True:
                conn, addr = server_sock.accept()
                threading.Thread(target=handle_connection, args=(conn, addr, protocol)).start()
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            try:
                server_sock.bind((HOST, PORT))
            except OSError as e:
                print(f"Eroare la legarea socket-ului broker-ului: {e}")
                sys.exit(1)

            print(f"Broker-ul rulează pe {HOST}:{PORT} folosind UDP.")

            while True:
                data, addr = server_sock.recvfrom(4096)
                threading.Thread(target=process_message, args=(data, addr, protocol)).start()


if __name__ == "__main__":
    main()
