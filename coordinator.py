import socketserver
import socket

class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024).strip()
        response = Coordinator.execute(data)
        print("sending:", response)
        self.request.send(bytes(response, 'utf-8'))
        return


class Coordinator:
    def __init__(self):
        Coordinator.nrBuckets = 0
        Coordinator.bucketList = {}
        Coordinator.dicc = {}
        address = ('localhost',0)
        self.server = socketserver.TCPServer(address, MyTCPHandler)
        ip, port = self.server.server_address #find out ip and port number
        print("My address is", ip, port)
        self.server.serve_forever()
		
    def execute(message):
        msg=message.decode("utf-8")
        msg.strip()
        lista = msg.split()
        print(lista)
        try:
            if lista[0] == "INSERT":
                # create ID for data( lista[2] )
                Coordinator.dicc[int(lista[1])] = lista[2]  # instead of user input, key will be hash result.
                # send to bucket
                # if bucket return overflow 
                #   split action taken

                # P.S. Need to use ID to hash, user does not enter which bucket to insert
                return "ACK"
            elif lista[0] == "QUERY":
                return Coordinator.dicc[int(lista[1])]
            elif lista[0] == "REGISTER":
                Coordinator.nrBuckets += 1
                Coordinator.bucketList[Coordinator.nrBuckets]="{0} {1}".format(
                    lista[1], lista[2])
                return "{}".format(Coordinator.nrBuckets)
            elif lista[0] == "POPULATE":
                return "POPULATION "+' '.join("{} {}".format(k,v) for k,v in Coordinator.bucketList.items())
            else:
                return "NOPE"
        except KeyError:
            return "key error"

    # split function
    def split()

			
coordinator = Coordinator()
