import socket
import sys
import threading
import os
import time
import subprocess
import ast
import signal


MyFiles = {}  # Mapa de ficheiros do nosso nodo
checker_thread_event = False #Variável que serve como evento para terminar as threads
cpp_process = None # Variável que guarda o sub processo do FS Transfer
lock = threading.Lock()

# Função que corre a thread "client_program"
def run_client_program():
        client_program(arg1, arg2)

# Função que corre o programa FS Transfer
def run_cpp_program():
    global cpp_process
    st = f'./sender {arg1}'
    cpp_process = subprocess.Popen(st, shell=True)  # Store the subprocess
    cpp_process.wait()  # Wait for the process to exit
# Função que termina o programa FS Transfer
def stop_cpp_process():
    global cpp_process
    if cpp_process:
        os.kill(cpp_process.pid+1, signal.SIGTERM)  # Terminate the process

# Função "checker": É uma thread pararela que está em constante contacto com o servidor e, através da
# pasta partilhada previamente definida, envia mensagens para o servidor de registo de ficheiros ("regf")
def checker(folder, socket):
    global checker_thread_event
    total_time = 0
    num_messages = 0
    time.sleep(0.5)
    while not checker_thread_event:
        with lock:
            try:
                files = os.listdir(folder)
                for file in files:
                    file_path = os.path.join(folder, file)
                    if os.path.isfile(file_path):
                        file_name = os.path.basename(file_path)
                        file_size = os.path.getsize(file_path)
                        MyFiles[file_name] = (file_size, file_path)

                        start_time = time.time()
                        message = f"regf {file_name} 1 {file_size}"
                        socket.send(message.encode())
                        socket.recv(1024).decode()
                        end_time = time.time()

                        time_taken = end_time - start_time
                        total_time += time_taken
                        num_messages += 1

                if num_messages > 0:
                    average_time_per_message = total_time / num_messages
                    message = f"rtt {average_time_per_message}"
                    socket.send(message.encode())
                time.sleep(3)


            except Exception as e:
                print(f"An error occurred: {e}")

# Função auxilar utilizada para calcular uma divisão dos segmentos por difrentes hosts quando possível, garantindo
# que o ficheiro é totalmente transferido
def find_segregation(socket_list, max_end):
    # Calcular o rtt total
    total_rtt = sum(rtt for (_, _, rtt) in socket_list)

    while True:
        # Calcula o tempo médio
        average_time_slice = total_rtt / len(socket_list)

        # Filtrar elementos tendo em conta o tempo médio
        filtered_list = [entry for entry in socket_list if
                         entry[2] <= average_time_slice * 1.85]  # Threshold de 1.85, não é possível ter um "2"

        if len(filtered_list) == len(socket_list):
            break  # Quando o filtro estiver aplicado, podemos continuar 

        socket_list = filtered_list  # Update com a nova lista

    # Sort por rtt ascendente 
    socket_list.sort(key=lambda x: x[2])

    selected_combinations = []
    total_bytes = 0
    current_start = 1  # Começar no ínicio do ficheiro

    # Determinar os segmentos que pedir a cada host
    j = len(socket_list) - 1

    for i in range(len(socket_list)):
        if i > j / 2:
            break

        temp = socket_list[i][2]
        socket_list[i] = (socket_list[i][0], socket_list[i][1], socket_list[j][2])
        socket_list[j] = (socket_list[j][0], socket_list[j][1], temp)

    total_rtt = sum(rtt for (_, _, rtt) in socket_list)
    for (address, port, rtt) in socket_list:
        bytes_for_host = int(round((rtt / total_rtt) * max_end))  # Rounding calculation

        remaining_bytes = max_end - total_bytes
        bytes_for_host = min(bytes_for_host, remaining_bytes)  # Adjust allocation if exceeding max_end

        selected_combinations.append((address[0], address[1], current_start, current_start + bytes_for_host - 1))
        total_bytes += bytes_for_host
        current_start += bytes_for_host

    # Pequeno ajuste caso não fique completo, isto porque as divisiões às vezes deixam pequenos restos
    if selected_combinations and selected_combinations[-1][3] != max_end:
        selected_combinations[-1] = (selected_combinations[-1][0], selected_combinations[-1][1],
                                     selected_combinations[-1][2], max_end)

    return selected_combinations

# Função que prepara os argumentos necessários para invocar o Fs Transfer. Para isso, à uma serie de passos intermédios,
# desde o uso
def set_receiver(socket_list_str, folder, file_name,socket):
    file_path = os.path.join(folder, file_name)


    if socket_list_str == "":
        print("Ficheiro não existe")
        return
    
    socket_list = ast.literal_eval(socket_list_str)
    # Verificar se o ficheiro já existe na nossa diretoria
    if file_name in MyFiles.keys():
        print("Ficheiro Já existe na pasta")
        return
    max_end = max(end for ((_, _), (_, end), _) in socket_list)
    selected_combinations = find_segregation(socket_list,max_end)

    print(selected_combinations)
    #Criar a string para executar o programa Fs Transfer
    cmd_string = f'./sender {folder} '

    for ((address, port,start, end)) in selected_combinations:
        if (end > max_end):
            max_end = end
        append = f'{address} {file_name} {start} {end} '
        cmd_string+=append

    cmd_string = cmd_string[:len(cmd_string)-1]

    if max_end != 0:
        # Função que corre o programa recetor, não confundir com a função que executa o programa que envia
        start_time = time.time()
        
        def run_cpp_program2():
            cmd = cmd_string
            process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
            error_output = process.stderr  # Caso exceda as tentativas
            
            if error_output:
                print(error_output)

        client_thread = threading.Thread(target=run_cpp_program2)
        client_thread.start()
        client_thread.join()
        end_time = time.time()
        execution_time = end_time - start_time  # Calculo do tempo de execução
        print("Transferencia demorou: ",round(execution_time,6))
        

# Thread principal do programa FS Node, é responsável por enviar mensagens ao servidor Tracker.
def client_program(folder, host):
    global checker_thread_event
    port = 9090  # Porta usada

    try:
        client_socket = socket.socket()

        client_socket.connect((host, port))
        print("SCS: Connected to", host)

    except:
        print("ERROR: Host não existe")
        return
    
    # registar nodo quando conectado
    message = "regn"
    client_socket.send(message.encode())

    ct = threading.Thread(target=checker, args=(folder, client_socket))
    ct.start()

    cpp_thread = threading.Thread(target=run_cpp_program)
    cpp_thread.start()


    message = input(" -> ")
    while True:
        aux = message.split()
        if aux[0] == "getf":
            client_socket.send(message.encode())
            data = client_socket.recv(1024).decode()
            set_receiver(data, folder,aux[1],client_socket)
            
        elif aux[0] =="unregn":
            client_socket.send(message.encode())
            client_socket.close()
            stop_cpp_process()
            checker_thread_event = True
            ct.join()
            break
        elif aux[0] =="status":
            client_socket.send(message.encode())
            
            
            
        message = input(" -> ")


if __name__ == '__main__':

    if len(sys.argv) == 3:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]

    else:
        print("Please provide arguments.")
        sys.exit(1)


    client_thread = threading.Thread(target=run_client_program)
    client_thread.start()
    
    
