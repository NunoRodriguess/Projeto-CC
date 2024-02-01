import socket
import threading
from datetime import datetime
import time


mapaFicheiros = {} #Dicionário que guarda a informação relativa aos ficheiros e aos nodos que têm os ficheiros
conectados = {} #Lista de nodos conectados
rtt_map= {} # Não implementado, mas serviria para manter um historial do "rtt" de cada nodo

lock = threading.Lock()

# Função que limpa entradas antigas, isto é, que não foram renovadas pelos clientes.
def fileMapManager():
    while True:
        time.sleep(5)  # Espera 5 segundos

        current_time = time.time()

        with lock:
            for file_name, clients in list(mapaFicheiros.items()):
                updated_clients = [(address, seg_info, timestamp) for address, seg_info,timestamp in clients if (current_time - timestamp.timestamp()) <= 10]

                if updated_clients:
                    mapaFicheiros[file_name] = updated_clients
                else:
                    del mapaFicheiros[file_name]

#Função handler. A grande parte da complexidade do servidor está expressa nesta função.
#Aqui as difrentes mensagens são devidamente processadas, o que geralmente envolve acessos à aos dicionários
def handle_client(client_socket, client_address):
    while True:
        data = client_socket.recv(1024).decode()

        if not data:
            break

        words = data.split(' ')
        if words[0] == 'regn':
            with lock:
                if client_address not in conectados.keys():
                    conectados[client_address] = (True,-1)

                elif client_address in conectados.keys() and conectados[client_address] != client_socket:
                    conectados[client_address] = (True,-1)
                    
            
        elif words[0] == 'unregn':
            with lock:
                to_be_removed = []
                if client_address in conectados.keys():
                    del conectados[client_address]
                    for  file_name in mapaFicheiros:
                        l = mapaFicheiros[file_name]
                        for (add,se,dat) in l:
                            if add == client_address:
                                l.remove((add,se,dat))
    

        elif words[0] == 'regf': # isto acaba por ser automáitco agora, não sei se vale a pena deixar o cliente mandar files
            with lock:
                if client_address in conectados.keys():
                    try:
                        integer1_seg = int(words[2])
                        integer2_seg = int(words[3])
                        file_name = words[1]

                        if file_name in mapaFicheiros.keys():

                            client_found = False
                            for i, (address, (seg1, seg2),date) in enumerate(mapaFicheiros[file_name]):
                                if address == client_address:

                                    mapaFicheiros[file_name][i] = (client_address,(integer1_seg, integer2_seg),datetime.now())
                                    client_found = True
                                    break

                            if not client_found:

                                mapaFicheiros[file_name].append((client_address,(integer1_seg, integer2_seg),datetime.now()))
                        else:

                            mapaFicheiros[file_name] = [(client_address,(integer1_seg, integer2_seg),datetime.now())]
                        
                        message = "Sucesso"
                        client_socket.send(str(message).encode())
                    except (IndexError, KeyError): 
                        pass    
               

        elif words[0] == 'getf':

            with lock:
                if client_address in conectados.keys():
                    try:
                        response = mapaFicheiros[words[1]]
                    except (IndexError):
                        response = "ERRO: Faltam campos na mensagem"
                        client_socket.send(response.encode())
                    except (KeyError):
                        response = "ERRO: Ficheiro não existe"
                        client_socket.send(response.encode())
                else:
                    response = "ERRO: Nodo não registado"

                result = [(addr, seg, conectados.get(addr, (False, None))[1]) for addr, seg, _ in response]
                client_socket.send(str(result).encode())
                
        elif words[0] == 'rtt':
            with lock:
                try:

                    float_rtt = float(words[1])
                    conectados[client_address] = (True,float_rtt)
                except (IndexError, ValueError) as e:
                    pass
                
        else:
            print(mapaFicheiros)
            print(conectados)
            print(rtt_map)

#Função que recebe conexões e invoca handlers
def server_program():
    host = socket.gethostname()
    port = 9090

    print("Host: ",host)

    server_socket = socket.socket()
    server_socket.bind((host, port))
    server_socket.listen(10)


    map_manager = threading.Thread(target=fileMapManager, args=())
    map_manager.start()

    while True:
        client_socket, client_address = server_socket.accept()
        print("Connection from: " + str(client_address))
        client_handler = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_handler.start()

if __name__ == '__main__':
    server_program()
