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
        Coordinator.totalBuckets = 1
        self.server.serve_forever()

    def split():
        oldBucket = Bucket.fs.splitPtr
        newBucket = Bucket.fs.splitPtr + 2**Bucket.fs.level
        print("old: {}; new: {}".format(oldBucket, newBucket))
        Coordinator.requestRehash(oldBucket, newBucket)
        Bucket.fs.splitPtr += 1
        if Bucket.fs.splitPtr == 2**Bucket.fs.level:
            Bucket.fs.level += 1
            Bucket.fs.splitPtr = 0
        print(Bucket.fs)
        return newBucket

    def requestRehash(oldBucket, newBucket):
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
                data = "REHASH {} {}".format(Bucket.fs.level, Bucket.fs.splitPtr)
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
        try:
            if command == "INSERT":
                Bucket.insert(int(lista[1]), lista[2])
                return "ACK"
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
                Coordinator.split()
                return "ACK"
            else:
                return "NOPE"
        except KeyError:
            return "key error"


coordinator = Coordinator(MyTCPHandler)
