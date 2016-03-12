import socket
import sys
from address import address
from file_state import FileState


coHost = "localhost"
coPort = int(sys.argv[1])

bucketAddress = {}
bucketAddress[0] = "{} {}".format(coHost, coPort)

fs = FileState()

def main():
    global bucketAddress
    global fs
    while True:
        data = input("command: ")
        if data.split()[0].upper() == "STOP":
            break       
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        destAddress = bucketAddress[0].split()
        destHost, destPort = destAddress[0], int(destAddress[1])
        if len(data.split()) > 1:
            key = int(data.split()[1])
            destBucket = address(key, fs)
            if destBucket not in bucketAddress:
                requestPopulate()
            destAddress = bucketAddress[destBucket].split()
            destHost, destPort = destAddress[0], int(destAddress[1])
            print(destHost, destPort)
        try:
            # Connect to server and send data
            sock.connect((destHost, destPort))
            sock.sendall(bytes(data + "\n", "utf-8"))
            print("Sent:     {}".format(data))
            # Receive data from the server
            received = str(sock.recv(1024), "utf-8")
        finally:
            #Close connection
            sock.close()
        #process reply
        reply = received.split()
        replyHandler(reply)
        print("Received: {}".format(received))


def requestPopulate():
    global bucketAddress
    data = "POPULATE"
    destAddress = bucketAddress[0].split()
    destHost, destPort = destAddress[0], int(destAddress[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((destHost, destPort))
        sock.sendall(bytes(data + "\n", "utf-8"))
        print("Sent:     {}".format(data))
        # Receive data from the server
        received = str(sock.recv(1024), "utf-8")
    finally:
        #Close connection
        sock.close()
    #process reply
    reply = received.split()
    replyHandler(reply)
    print("Received: {}".format(received))
    
def replyHandler(reply):
    global bucketAddress
    global fs
    if not reply:
        return
    if reply[0] == "POPULATION":
        i = 1
        while i < len(reply):
            bucketAddress[int(reply[i])] = " ".join([reply[i+1], reply[i+2]])
            i+=3
    elif reply[0] == "IAM":
        fs = FileState(int(reply[1]))
    

if __name__ == '__main__':
    main()
