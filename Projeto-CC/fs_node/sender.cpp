#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <filesystem> 
#include <net/if.h>
#include <ifaddrs.h>
#include <thread>
#include <stdlib.h>
#include <atomic>
#include <vector>
#include <algorithm>
#include <string>
#include <signal.h>

using namespace std;

std::string sharedFolderPath; // Pasta partilhada, é passada como argumento

std::atomic<bool> isReceiverThreadDone(false); // Serve para sincronizar threads

int udpSocket_serv  = -1; // Variável global que guarda a socket udp

#define MAX_SEGMENT_SIZE 1024 // tamanho máximo definido para os segmentos, pode ser alterado

#define MAX_TRYES 100 // número de tentativas que o programa está disposto a fazer

/*
* Função que conta o número de digitos de um inteiro, é utilizada para calcular o espaço gasto pelas tags.
*/
int NumDigits(int x)  
{  
    x = abs(x);  
    return (x < 10 ? 1 :   
        (x < 100 ? 2 :   
        (x < 1000 ? 3 :   
        (x < 10000 ? 4 :   
        (x < 100000 ? 5 :   
        (x < 1000000 ? 6 :   
        (x < 10000000 ? 7 :  
        (x < 100000000 ? 8 :  
        (x < 1000000000 ? 9 :  
        (x < 10000000000 ? 10 :  
        (x < 100000000000 ? 11 :  
        (x < 1000000000000 ? 12 :  
        (x < 10000000000000 ? 13 :  
        (x < 100000000000000 ? 14 :  
        (x < 1000000000000000 ? 15 :  
        16)))))))))))))));  
}

/*
* Função de grande complexidade que retransforma um array dinâmico que é passado por argumento
* De modo geral, o que esta função faz é partir os segmentos enviados por argumento em segmentos tendo em conta o MAX_SEGMENT_SIZE
* Assim, é mais facil de perceber que segmentos pequenos estão em falta (packet loss) e pedir mais eficientemente ao respetivo dono
*/

int segmentBlock(char**& slots, int& n) {
    int additionalSlots = 0;
    int currentIndex = n;

    for (int i = 0; i < n; i += 4) {
        int start = atoi(slots[i + 2]);
        int end = atoi(slots[i + 3]);

        int tagc = 1 + NumDigits(start);
        int blockSizec = MAX_SEGMENT_SIZE - tagc;
 
        if (start + blockSizec < end) {
         
            while (start < end) {
                int tag = 1 + NumDigits(start);
                int blockSize = MAX_SEGMENT_SIZE - tag;
                int blockEnd = start + blockSize > end ? end : start + blockSize;

                char* newSlot = (char*)malloc(500);
        
                sprintf(newSlot, "%d", blockEnd);

                if (currentIndex + 3 >= n) {
                    // Resize do array
                    int newSize = n + 4; // Slots são de 4 em 4
                    char** temp = (char**)realloc(slots, newSize * sizeof(char*));
                    if (temp == NULL) {
                        
                        
                        break;
                    }
                    slots = temp;
                    n = newSize;
                }

                slots[currentIndex] = strdup(slots[i]);
                slots[currentIndex + 1] = strdup(slots[i + 1]);
                slots[currentIndex + 2] = (char*)malloc(500);
                if (slots[currentIndex + 2] == NULL) {

                    
                    break;
                }
                sprintf(slots[currentIndex + 2], "%d", start);
                slots[currentIndex + 3] = (char*)malloc(500);
                if (slots[currentIndex + 3] == NULL) {

                    
                    
                    break;
                }
                sprintf(slots[currentIndex + 3], "%d", blockEnd);

                currentIndex += 4;
                start = blockEnd;
                additionalSlots += 1;
            }
            slots[i] = strdup("-1");
        }
    }

    return n + additionalSlots * 4;
}
/*
* Função que "limpa" os argumentos recebidos, e também retrasnforma o array num array dinâmico
*/
char** filterArguments(char* argv[], int argc, int& newArgc) {
    std::vector<char*> filteredArgs;


    for (int i = 0; i < argc; ++i) {
        
            // Alocar memória
            char* arg = new char[strlen(argv[i]) + 1];
            strcpy(arg, argv[i]);
            filteredArgs.push_back(arg);
        
    }

    // Criar novo array dinâmico
    char** dynamicArray = new char*[filteredArgs.size()];
    for (size_t i = 0; i < filteredArgs.size(); ++i) {
        dynamicArray[i] = filteredArgs[i];
    }

    // Novo tamanho
    newArgc = filteredArgs.size();

    return dynamicArray;
}

/*
* Função que envia o um segmento requirido para um destino definido.
* Aqui entram as "tags" que correspondem a um apêndice ( tendo em conta o MAX_SEGMENT_SIZE) colocados no
* começo dos bytes enviados. e.g "1:Isto é um exemplo".
*/

void send_file(const std::string& filename, int start, int end, const std::string& ip, int port) {
    // Verificar integridade da pasta partilhada

    if (sharedFolderPath.empty()) {
        std::cerr << "Shared folder path is not initialized\n";
        return;
    }
    
    std::string filepath = std::string(sharedFolderPath) + filename;

    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {

        return;
    }

    // Colocar na posição entendida
    file.seekg(start-1);

    int tag_size = NumDigits(start)+1; 
    // Criar socket udp
    int udpSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSocket < 0) {
        std::cerr << "Failed to create socket\n";
        return;
    }

    // Configurar os dados para enviar ao servidor
    struct sockaddr_in serverAddr;
    std::memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip.c_str(), &serverAddr.sin_addr) <= 0) {
        std::cerr << "Invalid IP address\n";
        close(udpSocket);
        return;
    }

    // Ler e enviar os segmentos
    int segmentNumber = start;  // Inicializar o segmento ao inicio

    char buffer[MAX_SEGMENT_SIZE];
    while (start < end) {
        int bytesToRead = std::min(MAX_SEGMENT_SIZE-tag_size, end - start+1);
        file.read(buffer, bytesToRead);
        int bytesRead = file.gcount(); // Número de bytes lido

        if (bytesRead <= 0) {
            break; // Erro de EOF ou problemas em aceder ao ficheiro
        }
        buffer[bytesRead] = '\0';
        // Preparar a tag tendo em conta o "start".
        std::string segment = std::to_string(segmentNumber++) + ":" + std::string(buffer, bytesRead);
        sendto(udpSocket, segment.c_str(), segment.size(), 0, (struct sockaddr*)&serverAddr, sizeof(serverAddr));
        
        start += bytesRead;
    }

    // Fechar o ficheiro
    file.close();
    close(udpSocket);
}

/*
* Esta função invoca a anterior para enviar o respetivo segmento. Além disso está sempre à espera de pedidos
*/

int receive_data(int udpSocket) {
    while (true) {
        char buffer[MAX_SEGMENT_SIZE];
        struct sockaddr_in clientAddr;
        socklen_t addrLen = sizeof(clientAddr);

        // Receive data
        int bytesReceived = recvfrom(udpSocket, buffer, sizeof(buffer), 0, (struct sockaddr *)&clientAddr, &addrLen);
        if (bytesReceived < 0) {
            std::cerr << "Failed to receive data\n";
            return 1;
        }

        buffer[bytesReceived] = '\0'; // Colocar caracter null
       
        std::istringstream iss(buffer);
        std::string command, filename, start, end, ip, port;

        if (!(iss >> command >> filename >> start >> end >> ip >> port)) {
            std::cerr << "Invalid message format\n";
            return 1;
        }


        int startInt = std::stoi(start);
        int endInt = std::stoi(end);
        int portInt = std::stoi(port);

        if (command == "sendFileTo") {
            send_file(filename, startInt, endInt, ip, portInt);
        } else {
            std::cerr << "Unknown command\n";
        }
    }

    return 0;
}

/*
* Função que define a terminação do servidor
*/

void sigterm_handler(int signum) {
   
    if (udpSocket_serv != -1) {
        close(udpSocket_serv); // Fechar socket
    }
    exit(0);
}

/*
* Função com comportamento de servidor. Aqui temos a criação de uma socket UDP e a invocação da função anterior
*/

int sender_mode(int port) {
    udpSocket_serv = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSocket_serv  < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    struct sockaddr_in serverAddr;
    std::memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = INADDR_ANY; // Bind to any available address

    if (bind(udpSocket_serv , reinterpret_cast<struct sockaddr *>(&serverAddr), sizeof(serverAddr)) < 0) {
        std::cerr << "Failed to bind socket\n";
        close(udpSocket_serv );
        return 1;
    }

    // Register SIGTERM signal handler
    signal(SIGTERM, sigterm_handler);
    

    int r = receive_data(udpSocket_serv );
    close(udpSocket_serv);
    return r;
}

/*
* Função que recebe segmentos.
* Aqui vemos a lógica necessária para montar o segmento, assim como a deteção de perdas de pacotes.
*/
void receive_segments(int udpSocket, std::unordered_map<int,std::string> & fileData, const std::string& sharedFolderPath, const std::string& filename, int end,int max_segements) {
     int current_segments =1;
     int timeoutSeconds = 5;
    
    while (current_segments<=max_segements) {
        char buffer[MAX_SEGMENT_SIZE];
        struct sockaddr_in senderAddr;
        socklen_t addrLen = sizeof(senderAddr);
        
        
        fd_set readfds;
        struct timeval timeout;

        FD_ZERO(&readfds);
        FD_SET(udpSocket, &readfds);

        timeout.tv_sec = timeoutSeconds;
        timeout.tv_usec = 0;

        int activity = select(udpSocket + 1, &readfds, nullptr, nullptr, &timeout);

        if (activity == -1) {
            perror("Error in select");
            break;
        } else if (activity == 0) {
            
            break;
        } else {
            if (FD_ISSET(udpSocket, &readfds)) {
                     int bytesReceived = recvfrom(udpSocket, buffer, sizeof(buffer), 0, (struct sockaddr *)&senderAddr, &addrLen);
        if (bytesReceived < 0) {
            
            perror("Error receiving data");
            break;
        }
        std::string receivedMessage(buffer, bytesReceived);
   
      
        size_t delimiterPos = receivedMessage.find(':');
        if (delimiterPos != std::string::npos) {
                std::string segmentNumberStr = receivedMessage.substr(0, delimiterPos);
                std::string segmentContent = receivedMessage.substr(delimiterPos + 1);

            int segmentNumber = std::stoi(segmentNumberStr);

            fileData.insert(std::make_pair(segmentNumber,segmentContent));

       
      }
    else {
    std::cerr << "Invalid segment format: " << receivedMessage << std::endl;
    }
            }
        }
       
        current_segments++;
    }
  
    isReceiverThreadDone = true;
    
}

/*
* Função que inicia a thread com "receive_segments"
*/
void receiver_thread(int udpSocket, std::unordered_map<int,std::string> & fileData, const std::string& sharedFolderPath, const std::string& filename,int end,int max_segements) {

  
    receive_segments(udpSocket, fileData, sharedFolderPath, filename,end,max_segements);
}

/*
* Função que pede aos servidores pelos segmentos.
*/
int receiver_mode(char** slots, int n, std::unordered_map<int,std::string> & fileData,int how_many_segments) {
    int udpSocket;
    struct sockaddr_in receiverAddr;
    struct ifaddrs *addrs, *tmp;

    // Cria Socket Udp
    udpSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSocket < 0) {
        perror("Error opening socket");
        exit(EXIT_FAILURE);
    }
    
    // Defenir endereço e porta de receção
    memset(&receiverAddr, 0, sizeof(receiverAddr));
    getifaddrs(&addrs);

    // Procurar um endereço livre (sen ser o de loopback)
    for (tmp = addrs; tmp != NULL; tmp = tmp->ifa_next) {
        if (tmp->ifa_addr && tmp->ifa_addr->sa_family == AF_INET) {
            struct sockaddr_in *pAddr = (struct sockaddr_in *)tmp->ifa_addr;
            if (strcmp(inet_ntoa(pAddr->sin_addr), "127.0.0.1") != 0 && strcmp(inet_ntoa(pAddr->sin_addr), "0.0.0.0") != 0) {
                receiverAddr.sin_family = AF_INET;
                receiverAddr.sin_port = htons(8081); // default 8081
                receiverAddr.sin_addr = pAddr->sin_addr;
                break;
            }
        }
    }
         int opt = 1;
        if (setsockopt(udpSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
                 perror("setsockopt");

        }
    if (bind(udpSocket, (struct sockaddr *)&receiverAddr, sizeof(receiverAddr)) < 0) {
        std::cerr << "Failed to bind socket\n";
        close(udpSocket);
        return 1;
    }
    // inciar a thread que recebe
    std::thread receiver(receiver_thread, udpSocket, std::ref(fileData), sharedFolderPath, slots[(n - 3)],atoi(slots[(n-1)]),how_many_segments);
    receiver.detach(); 

    for (int i = 0; i < n; i += 4) {
        // Preparar a mensagem: sendFileTo filename start end thisIp thisPort
        if (slots[i]=="-1")
            continue;

         int udpSocket2 = socket(AF_INET, SOCK_DGRAM, 0);
         if (udpSocket2 < 0) {
        perror("Error opening socket");
        exit(EXIT_FAILURE);
        } 

        struct sockaddr_in senderAddr;
        memset(&senderAddr, 0, sizeof(senderAddr));
        senderAddr.sin_family = AF_INET;
        senderAddr.sin_port = htons(8080); // Use the default port 8080
        inet_pton(AF_INET, slots[i], &senderAddr.sin_addr);
        char message[MAX_SEGMENT_SIZE];
  
        snprintf(message, MAX_SEGMENT_SIZE, "sendFileTo %s %s %s %s 8081", slots[(i+1)], slots[(i+2)], slots[(i+3)], inet_ntoa(receiverAddr.sin_addr)); // filename //start //end //ip//port
        // Mandar a mensagem ao cliente no array "slots"
        if (sendto(udpSocket2, message, strlen(message), 0, (struct sockaddr*)&senderAddr, sizeof(senderAddr)) < 0) {
            perror("Error sending message");
            exit(EXIT_FAILURE);
        }
    }
    // esperar pela receção
    while (!isReceiverThreadDone) {
        sleep(0.1);
    }
    isReceiverThreadDone=false;
    // Verificar se já temos todos os segmentos, caso não a temos uma lógica difrente
    if (fileData.size() == how_many_segments) {
                std::string sharedFilePath = sharedFolderPath +  slots[(n - 3)]; // Use the filename provided
                std::string fileContent ="";
                std::vector<int> keys;
                for (const auto& pair : fileData) {
                    keys.push_back(pair.first);
                }
                std::sort(keys.begin(), keys.end());
                 for (const auto& key : keys) {
                    fileContent +=fileData[key];
                  }
                std::ofstream outputFile(sharedFilePath, std::ios::app);
                if (outputFile.is_open()) {
                    outputFile << fileContent;
                    outputFile.close();
                } else {
                    

                }
                
        }else {
    // Marcar os slots já recebidos com "-1" no host
    char*stringArray[n];
    for (int i = 0; i < n; i += 4) {
       
        int start = atoi(slots[i + 2]);
        int end = atoi(slots[i + 3]);
        while (start < end) {
            int tag = 1 + NumDigits(start);
            int this_segment = MAX_SEGMENT_SIZE - tag;
            if (fileData.find(start) != fileData.end()) {

               
                slots[i] = strdup("-1");
                break;
            }
            start = start + this_segment;
        }
    }

    
    close(udpSocket);
    return 1;
}
    close(udpSocket);
    return 0;
 }

/*
* Distinção entre funcionalidades: Tanto pode ser o "servidor que envia segmentos" como um "cliente que recebe segmentos"
*/
int main (int argc, char* argv[]) {

    sharedFolderPath = argv[1];

    if (sharedFolderPath.back() != '/') {
        sharedFolderPath += '/';
    }

    if (argc == 2){
       
        sender_mode(8080); 
    }
    else{
        std::unordered_map<int,std::string> fileData; // iniciar o mapa com os segmentos
        int newArgc;
        char** filteredArray = filterArguments(argv+2, argc-2, newArgc);
        segmentBlock(filteredArray,newArgc);
        int how_many_segments =0;
            for (int j = 0; j < newArgc; j += 4) {
                if (strcmp(filteredArray[j],"-1")){
                    how_many_segments++;
                    
                }
                continue;
                    
            }
        int flag = receiver_mode(filteredArray,newArgc,fileData,how_many_segments);
        int tryes = 0;
        // continuar até receber o pedido
        while(flag !=0 && tryes < MAX_TRYES){
            flag = receiver_mode(filteredArray,newArgc,fileData,how_many_segments);
            tryes++;
        }
        if (tryes == MAX_TRYES){
            std::cerr << "Numero de tentavias expirou" << std::endl;
            return 1;
        }
    }

    return 0;
}