import socket
import sys
from address import address


HOST = "localhost"
port = int(sys.argv[1])

bucketAddress = {}
bucketAddress[0] = "{} {}".format(HOST, port)

while True:
    data = input("command: ")
    if data.split()[0].upper() == "STOP":
        break       
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
    # Connect to server and send data
        sock.connect((HOST, port))
        sock.sendall(bytes(data + "\n", "utf-8"))
    # Receive data from the server
        received = str(sock.recv(1024), "utf-8")
    finally:
        #Close connection
        sock.close()
    #process reply
    reply = received.split()
    if reply[0] == "POPULATION":
        i = 1
        while i < len(reply):
            bucketAddress[int(reply[i])] = " ".join([reply[i+1], reply[i+2]])
            i+=3
    print("Sent:     {}".format(data))
    print("Received: {}".format(received))
#     print(bucketAddress)
