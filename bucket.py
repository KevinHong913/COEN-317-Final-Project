import socketserver
import socket
import sys



class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Echo the back to the client
        data = self.request.recv(1024).strip()
        response = Bucket.execute(data)
        print("sending:", response)
        self.request.send(bytes(response, 'utf-8'))
        return


class Bucket:
    def __init__(self):
        HOST = "localhost"
        port = int(sys.argv[1])
        print(HOST, port)
        Bucket.dicc = {}
        address = ('localhost',0)
        self.server = socketserver.TCPServer(address, MyTCPHandler)
        myIP, myPort = self.server.server_address #find out ip and port number
        print("My address is", myIP, myPort)
        self.register(HOST, port, myIP, myPort)
        print("registered")
        self.server.serve_forever()

    def register(self, coHost, coPort, myHost, myPort):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
	# Connect to coordinator
            data = "REGISTER {0} {1}".format(myHost, myPort)
            sock.connect((coHost, coPort))
            sock.sendall(bytes(data + "\n", "utf-8"))
        # Receive bucketnumber from Coordinator
            received = str(sock.recv(1024), "utf-8")
            print(received)
        finally:
            sock.close()
		
    def execute(message):
        msg=message.decode("utf-8")
        msg.strip()
        lista = msg.split()
        print(lista)
        try:
            if lista[0] == "INSERT":
                Coordinator.dicc[int(lista[1])] = lista[2]
                return "ACK"
            elif lista[0] == "QUERY":
                return Coordinator.dicc[int(lista[1])]
            else:
                return "NOPE"
        except KeyError:
            return "key error"
			
bucket = Bucket()
