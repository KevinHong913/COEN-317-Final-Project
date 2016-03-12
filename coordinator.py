import socketserver
import socket
from file_state import FileState
from address import address
from bucket import Bucket


class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024).strip()
        response = Coordinator.execute(data)
        print("sending:", response)
        self.request.send(bytes(response, 'utf-8'))
        return


class Coordinator(Bucket):
    def __init__(self, tcpHandler):
        Bucket.__init__(self, tcpHandler)
        Bucket.bucketNbr = 0
        Bucket.bucketList[0] = "{} {}".format(self.myHost, self.myPort)
        Bucket.coHost, Bucket.coPort = self.myHost, self.myPort
        Coordinator.totalBuckets = 1
        self.server.serve_forever()

    def split():
        oldBucket = Bucket.fs.splitPtr
        newBucket = Bucket.fs.extent
        print("old: {}; new: {}".format(oldBucket, newBucket))
        if newBucket not in Bucket.bucketList:
            return "SpaceLimitExceeded"
        Bucket.fs = FileState(newBucket+1)
        Coordinator.requestRehash(oldBucket)
        print(Bucket.fs)
        return "ACK"

    def requestRehash(oldBucket):
        if oldBucket == Bucket.bucketNbr:
            Bucket.rehash(Bucket.fs)
        else:
            bucketAddress = Bucket.bucketList[oldBucket].split()
            bucketHost, bucketPort = bucketAddress[0], int(bucketAddress[1])
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Connect to server and send data
                sock.connect((bucketHost, bucketPort))
                print("Connected to {}:{}".format(bucketHost, bucketPort))
                data = "REHASH {}".format(Bucket.fs.extent)
                sock.sendall(bytes(data + "\n", "utf-8"))
                received = str(sock.recv(1024), "utf-8")
            finally:
                #Close connection
                sock.close()


    def execute(message):
        msg=message.decode("utf-8")
        msg.strip()
        lista = msg.split()
        print(lista)
        command = lista[0].upper()
        if len(lista) > 1 and command in ("INSERT", "QUERY"):
            key = int(lista[1])
            location = address(key, Bucket.fs)
            if location != Bucket.bucketNbr:
                return Bucket.forward(location, msg)
        try:
            if command == "INSERT":
                return Coordinator.insert(int(lista[1]), lista[2])
#                 return "ACK"
            elif command == "QUERY":
                return Bucket.query(int(lista[1]))
            elif command == "REGISTER":
                bucketNbr = Coordinator.totalBuckets
                Coordinator.totalBuckets += 1
                Bucket.bucketList[bucketNbr]="{0} {1}".format(
                    lista[1], lista[2])
                print(Bucket.bucketList)
                return "{}".format(bucketNbr)
            elif command == "POPULATE":
                return "POPULATION "+' '.join("{} {}".format(k,v) for k,v in Bucket.bucketList.items())
            elif command == "SPLIT":
                return Coordinator.split()
            else:
                return "NOPE"
        except KeyError:
            return "key error"

    def insert(key, val):
        Bucket.dicc[key] = val
        result = "ACK"
        if len(Bucket.dicc) > 2:
            result = Coordinator.split()
        return result


coordinator = Coordinator(MyTCPHandler)
