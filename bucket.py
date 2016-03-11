import socketserver
import socket
import sys
from multiprocessing import Pool
from address import address
from file_state import FileState

class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024).strip()
        response = Bucket.execute(data)
        print("sending:", response)
        self.request.send(bytes(response, "utf-8"))
        return


class Bucket:
    def __init__(self, tcpHandler):
        Bucket.fs = FileState()
        Bucket.bucketNbr = None
        Bucket.coHost = "localhost"
        Bucket.coPort = int(sys.argv[1]) if len(sys.argv)>=2 else None
        print(Bucket.coHost, Bucket.coPort)
        Bucket.dicc = {}
        Bucket.bucketList = {}
        address = ('localhost',0)
        self.server = socketserver.TCPServer(address, tcpHandler)
        self.myHost, self.myPort = self.server.server_address #find out ip and port number
        print("My address is", self.myHost, self.myPort)

    def register(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try: 
            # Connect to coordinator
            data = "REGISTER {0} {1}".format(self.myHost, self.myPort)
            sock.connect((Bucket.coHost, Bucket.coPort))
            sock.sendall(bytes(data + "\n", "utf-8"))
            # Receive bucketnumber from Coordinator
            received = str(sock.recv(1024), "utf-8")
            print(received)
            Bucket.bucketNbr = int(received)
            Bucket.fs = FileState(Bucket.bucketNbr+1)
            print(Bucket.fs)
            Bucket.bucketList[Bucket.bucketNbr] = '{} {}'.format(self.myHost, self.myPort)
        finally:
            sock.close()
        print("registered")
        self.server.serve_forever()
        
    def execute(message):
        msg=message.decode("utf-8")
        msg.strip()
        lista = msg.split()
        print(lista)
        command = lista[0].upper()
        try:
            if command == "INSERT":
                Bucket.insert(int(lista[1]), lista[2])
                return "ACK"
            elif command == "QUERY":
                return Bucket.query(int(lista[1]))
            elif command == "POPULATION":
                Bucket.population(lista)
                return "ACK"
            elif command == "REHASH":
                Bucket.fs = FileState(int(lista[1]))
                Bucket.rehash(Bucket.fs)
                return "ACK"
            else:   #TODO
                return "NOPE"
        except KeyError:
            return "key error"

    def insert(key, val):
        Bucket.dicc[key] = val
        if len(Bucket.dicc) > 2 and Bucket.bucketNbr > 0:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Connect to server and send data
                sock.connect((Bucket.coHost, Bucket.coPort))
                data = "SPLIT"
                sock.sendall(bytes(data + "\n", "utf-8"))
                # Receive data from the server
                received = str(sock.recv(1024), "utf-8")
            finally:
                #Close connection
                sock.close()

    def query(key):
        return Bucket.dicc[key]

    def population(reply):
        print("Populating...")
        i = 1
        while i < len(reply):
            Bucket.bucketList[int(reply[i])] = " ".join([reply[i+1], reply[i+2]])
            i+=3
        print(Bucket.bucketList)

    def rehash(fs):
        # rehash each key
        print("Rehashing")
        print(fs)
        deleteList = []
        for key in Bucket.dicc:
            location = address(key, fs)
            if location == Bucket.bucketNbr:
                print("No need to rehash for key {}".format(key))
                continue
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if location not in Bucket.bucketList:
                try:
                    # Connect to server and send data
                    sock.connect((Bucket.coHost, Bucket.coPort))
                    data = "POPULATE"
                    sock.sendall(bytes(data + "\n", "utf-8"))
                    # Receive data from the server
                    received = str(sock.recv(1024), "utf-8")
                finally:
                    #Close connection
                    sock.close()
                #process reply
                reply = received.split()
                if reply[0] == "POPULATION":
                    Bucket.population(reply)
            destAddress = Bucket.bucketList[location].split()
            destHost, destPort = destAddress[0], int(destAddress[1])
            try:
                sock.connect((destHost, destPort))
                data = "INSERT {} {}".format(key, Bucket.dicc[key])
                sock.sendall(bytes(data + "\n", "utf-8"))
                received = str(sock.recv(1024), "utf-8")
                if received == "ACK":
                    deleteList.append(key)
            finally:
                #Close connection
                sock.close()
            print(received)
        for key in deleteList:
            Bucket.dicc.pop(key)

             

if __name__ == '__main__':            
    bucket = Bucket(MyTCPHandler)
    bucket.register()

