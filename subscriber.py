import socket
import json
import threading
import sys

# Solicită protocolul de transport
protocol = input("Alege protocolul de transport (TCP/UDP): ").upper()
if protocol not in ['TCP', 'UDP']:
    print("Protocol invalid. Alege TCP sau UDP.")
    sys.exit()

# Solicită numele unic al subscriber-ului
subscriber_name = input("Introdu numele unic al subscriber-ului: ")

# Adresa și portul broker-ului
broker_address = ('172.20.10.4', 10000)


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


local_ip = get_local_ip()

# Adresa și portul subscriber-ului
HOST = 'localhost'
PORT = 0  # Permitem sistemului să aloce un port liber

subscribed_topics = []


def send_request(data):
    serialized_data = json.dumps(data).encode('utf-8')

    if protocol == 'TCP':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(broker_address)
            sock.sendall(serialized_data)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(serialized_data, broker_address)


def receive_initial_data(server_sock):
    if protocol == 'TCP':
        conn, addr = server_sock.accept()
        data = conn.recv(4096)
        conn.close()
        response = json.loads(data.decode('utf-8'))

        return response
    else:
        data, addr = server_sock.recvfrom(4096)  # UDP nu necesită acceptarea conexiunilor
        response = json.loads(data.decode('utf-8'))

        return response


def connect_to_broker():
    global PORT
    global subscribed_topics

    if protocol == 'TCP':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            try:
                server_sock.bind((HOST, 0))  # Alocă un port liber
            except OSError as e:
                print(f"Eroare la legarea socket-ului: {e}")
                sys.exit(1)

            PORT = server_sock.getsockname()[1]
            server_sock.listen()

            # Trimitem cererea de conectare către broker
            data = {
                'action': 'subscriber_connect',
                'subscriber': subscriber_name,
                'protocol': protocol,
                'address': [local_ip, PORT]
            }
            send_request(data)

            # Așteptăm datele inițiale de la broker
            response = receive_initial_data(server_sock)

            if response.get('action') == 'subscriber_data':
                subscribed_topics = response.get('topics', [])
                offline_messages = response.get('offline_messages', [])

                if subscribed_topics:
                    print(f"Te-ai reabonat automat la topic-urile: {', '.join(subscribed_topics)}")
                else:
                    print("Nu ești abonat la niciun topic.")

                if offline_messages:
                    print("Ai mesaje nelivrate:")
                    for msg in offline_messages:
                        print(f"[Offline] Topic: {msg['topic']} - {msg['message']}")
            else:
                print("Eroare la conectarea cu broker-ul.")
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            try:
                server_sock.bind((HOST, 0))  # Alocă un port liber
            except OSError as e:
                print(f"Eroare la legarea socket-ului UDP: {e}")
                sys.exit(1)

            PORT = server_sock.getsockname()[1]

            # Trimitem cererea de conectare către broker
            data = {
                'action': 'subscriber_connect',
                'subscriber': subscriber_name,
                'protocol': protocol,
                'address': [local_ip, PORT]
            }
            send_request(data)

            # Aștept răspuns de la broker
            response = receive_initial_data(server_sock)

            if response.get('action') == 'subscriber_data':
                subscribed_topics = response.get('topics', [])
                offline_messages = response.get('offline_messages', [])

                if subscribed_topics:
                    print(f"Te-ai reabonat automat la topic-urile: {', '.join(subscribed_topics)}")
                else:
                    print("Nu ești abonat la niciun topic.")

                if offline_messages:
                    print("Ai mesaje nelivrate:")
                    for msg in offline_messages:
                        print(f"[Offline] Topic: {msg['topic']} - {msg['message']}")
            else:
                print("Eroare la conectarea cu broker-ul.")


def subscribe_to_topic(topic):
    if topic in subscribed_topics:
        print(f"Ești deja abonat la '{topic}'.")
        return

    data = {
        'action': 'subscribe',
        'subscriber': subscriber_name,
        'topic': topic,
        'address': [HOST, PORT]
    }

    send_request(data)

    subscribed_topics.append(topic)
    print(f"Te-ai abonat la '{topic}'.")


def receive_messages():
    if protocol == 'TCP':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            try:
                server_sock.bind((HOST, PORT))
            except OSError as e:
                print(f"Eroare la legarea socket-ului pe portul {PORT}: {e}")
                sys.exit(1)

            server_sock.listen()

            while True:
                conn, addr = server_sock.accept()
                data = conn.recv(4096)
                message = json.loads(data.decode('utf-8'))
                print(f"\n[Mesaj nou] Topic: {message['topic']} - {message['message']}")
                conn.close()
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            try:
                server_sock.bind((HOST, PORT))
            except OSError as e:
                print(f"Eroare la legarea socket-ului UDP pe portul {PORT}: {e}")
                sys.exit(1)

            while True:
                data, addr = server_sock.recvfrom(4096)
                message = json.loads(data.decode('utf-8'))
                print(f"\n[Mesaj nou] Topic: {message['topic']} - {message['message']}")


def main():
    connect_to_broker()
    threading.Thread(target=receive_messages, daemon=True).start()

    try:
        while True:
            print("\n1. Abonează-te la un topic")
            print("2. Vezi topic-urile abonate")
            print("3. Ieșire")

            choice = input("Alege o opțiune: ")

            if choice == '1':
                topic = input("Introdu numele topic-ului: ")
                subscribe_to_topic(topic)
            elif choice == '2':
                if subscribed_topics:
                    print(f"Ești abonat la: {', '.join(subscribed_topics)}")
                else:
                    print("Nu ești abonat la niciun topic.")
            elif choice == '3':
                print("Ieșire din subscriber.")
                break
            else:
                print("Opțiune invalidă.")
    except KeyboardInterrupt:
        print("\nIeșire din subscriber.")
    except Exception as e:
        print(f"Eroare: {e}")


if __name__ == "__main__":
    main()
