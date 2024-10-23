import socket
import json
import threading
import sys

# Solicită protocolul de transport
protocol = input("Alege protocolul de transport (TCP/UDP): ").upper()
if protocol not in ["TCP", "UDP"]:
    print("Protocol invalid. Alege TCP sau UDP.")
    sys.exit()

# Solicită numele unic al publisher-ului
publisher_name = input("Introdu numele unic al publisher-ului: ")

# Adresa și portul broker-ului
broker_address = ("172.20.10.4", 10000)


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '172.0.0.1'
    finally:
        s.close()
    return IP


local_ip = get_local_ip()

# Adresa locală și portul publisher-ului
HOST = "172.20.10.8"
PORT = 10000  # Permitem sistemului să aloce un port liber

topics = []


def send_request(data):
    serialized_data = json.dumps(data).encode("utf-8")

    if protocol == "TCP":
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(broker_address)
            sock.sendall(serialized_data)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(serialized_data, broker_address)


def receive_response(server_sock):
    if protocol == "TCP":
        conn, addr = server_sock.accept()
        data = conn.recv(4096)
        conn.close()
        response = json.loads(data.decode("utf-8"))

        return response
    else:
        data, addr = server_sock.recvfrom(4096)
        response = json.loads(data.decode("utf-8"))

        return response


def connect_to_broker():
    global PORT
    global topics

    if protocol == "TCP":
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            try:
                server_sock.bind((HOST, 0))  # Alocă un port liber
            except OSError as e:
                print(f"Eroare la legarea socket-ului: {e}")
                sys.exit(1)

            PORT = server_sock.getsockname()[1]
            server_sock.listen()

            # Acum că avem PORT-ul, trimitem cererea către broker
            data = {
                "action": "publisher_connect",
                "publisher": publisher_name,
                "protocol": protocol,
                "address": [local_ip, PORT],
            }
            print(data)
            send_request(data)

            # Așteptăm răspunsul de la broker
            response = receive_response(server_sock)

            if response.get("action") == "publisher_topics":
                topics = response.get("topics", [])

                if topics:
                    print(f"Topic-uri încărcate de la broker: {', '.join(topics)}")
                else:
                    print("Nu există topic-uri create anterior.")
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

            # Trimitere cerere de conectare la broker
            data = {
                "action": "publisher_connect",
                "publisher": publisher_name,
                "protocol": protocol,
                "address": [local_ip, PORT],
            }
            send_request(data)

            # Aștept răspuns de la broker
            response = receive_response(server_sock)

            if response.get("action") == "publisher_topics":
                topics = response.get("topics", [])

                if topics:
                    print(f"Topic-uri încărcate de la broker: {', '.join(topics)}")
                else:
                    print("Nu există topic-uri create anterior.")
            else:
                print("Eroare la conectarea cu broker-ul.")


def create_topic():
    topic = input("Introdu numele noului topic: ")

    if topic in topics:
        print(f"Topic-ul '{topic}' există deja.")
    else:
        data = {"action": "create_topic", "publisher": publisher_name, "topic": topic}
        send_request(data)
        topics.append(topic)
        print(f"Topic-ul '{topic}' a fost creat.")


def publish_message():
    if not topics:
        print("Nu există topic-uri. Creează unul mai întâi.")
        return

    print(f"Topic-uri disponibile: {', '.join(topics)}")
    topic = input("Alege un topic: ")

    if topic not in topics:
        print("Topic invalid.")
        return

    message = input("Introdu mesajul: ")
    data = {
        "action": "publish",
        "publisher": publisher_name,
        "topic": topic,
        "message": message,
    }

    send_request(data)
    print("Mesaj trimis.")


def main():
    try:
        connect_to_broker()
        while True:
            print("\n1. Creează un topic nou")
            print("2. Trimite un mesaj")
            print("3. Ieșire")

            choice = input("Alege o opțiune: ")

            if choice == "1":
                create_topic()
            elif choice == "2":
                publish_message()
            elif choice == "3":
                print("Ieșire din publisher.")
                break
            else:
                print("Opțiune invalidă.")
    except KeyboardInterrupt:
        print("\nIeșire din publisher.")
    except Exception as e:
        print(f"Eroare: {e}")


if __name__ == "__main__":
    main()
