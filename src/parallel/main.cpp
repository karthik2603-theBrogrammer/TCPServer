#include "Request.hpp"
#include "Objects.hpp"


#include <iostream>
#include <cstdlib>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sstream>


#define NUM_THREADS 100 


void parse_buffer(std::queue<Request> bufferForRequests, int CLIENT_SOCKET)
{
    while (!bufferForRequests.empty())
    {
        Request reqC = bufferForRequests.front();
        int n;
        std::string finalRes;
        if (reqC.action == "COUNT")
        {
            std::lock_guard<std::mutex> lock(KV_DATASTORE_MUTEX);
            int res = KV_DATASTORE.size();
            finalRes = std::to_string(res) + "\n";
        }
        else if (reqC.action == "READ")
        {
            std::lock_guard<std::mutex> lock(KV_DATASTORE_MUTEX);
            if (KV_DATASTORE.find(reqC.key) == KV_DATASTORE.end())
            {
                finalRes = "NULL\n";
            }
            else
            {
                finalRes = KV_DATASTORE[reqC.key] + "\n";
            }
        }
        else if (reqC.action == "DELETE")
        {
            std::lock_guard<std::mutex> lock(KV_DATASTORE_MUTEX);
            if (KV_DATASTORE.find(reqC.key) == KV_DATASTORE.end())
            {
                finalRes = "NULL\n";
            }
            else
            {
                KV_DATASTORE.erase(reqC.key);
                finalRes = "FIN\n";
            }
        }
        else if (reqC.action == "WRITE")
        {
            std::lock_guard<std::mutex> lock(KV_DATASTORE_MUTEX);
            KV_DATASTORE[reqC.key] = reqC.value;
            finalRes = "FIN\n";
        }
        else if (reqC.action == "END")
        {
            close(CLIENT_SOCKET);
            return;
        }
        else
        {
            finalRes = reqC.action + " \n";
        }
        n = write(CLIENT_SOCKET, finalRes.c_str(), finalRes.size());
        if (n < 0)
        {
            perror("Error while writing to socket \n");
            close(CLIENT_SOCKET);
            pthread_exit(NULL);
        }
        bufferForRequests.pop();
    }
}

std::queue<Request> stream_input(std::istream &inputStream)
{
    std::queue<Request> reqs;
    std::string line;
    while (std::getline(inputStream, line))
    {
        Request req;
        req.action = line;

        if (line == "READ")
        {
            if (!std::getline(inputStream, req.key))
            {
                // std::cerr << "Invalid " << line << " command: Missing key" << std::endl;
                continue;
            }
        }
        else if (line == "WRITE")
        {
            if (!std::getline(inputStream, req.key) || !std::getline(inputStream, req.value))
            {
                // std::cerr << "Invalid WRITE command: Missing key or value" << std::endl;
                continue;
            }
            auto pos = req.value.find(':');
            if (pos != std::string::npos)
            {
                req.value.erase(pos, 1);
            }
        }
        else if (line == "DELETE")
        {
            if (!std::getline(inputStream, req.key))
            {
                // std::cerr << "Invalid " << line << " command: Missing key" << std::endl;
                continue;
            }
        }

        reqs.push(req);
    }
    return reqs;
}

void worker_thread(void *arg)
{
    while (true)
    {
        int client_socket;
        {
            std::lock_guard<std::mutex> lock(client_sockets_mutex);
            if (client_sockets.empty())
            {
                continue;
            }
            client_socket = client_sockets.front();
            client_sockets.pop();
        }
        char buffer[256];
        int n;
        bzero(buffer, 256);
        n = read(client_socket, buffer, 255);
        if (n < 0)
        {
            perror("ERROR reading from socket \n");
            close(client_socket);
            pthread_exit(NULL);
        }

        std::istringstream inputBuffer(buffer);
        std::queue<Request> bufferForRequests = stream_input(inputBuffer);

        parse_buffer(bufferForRequests, client_socket);

        if (n < 0)
        {
            perror("ERROR writing to socket \n");
            close(client_socket);
            pthread_exit(NULL);
        }
        close(client_socket);
    }
}

int main(int argc, char **argv)
{
    int PORT;
    int SOCK_FD;
    int CLI_LEN;
    int NEW_SOCK_FD;
    struct sockaddr_in SERVER_ADDRESS, CLIENT_ADDRESS;

    if (argc != 2)
    {
        std::cerr << "Use the command as : " << argv[0] << " <port_no>" << std::endl;
        exit(1);
    }

    PORT = atoi(argv[1]);

    SOCK_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (SOCK_FD < 0)
    {
        // std::cerr << "An error while opening sockets\n"<< std::endl;
        exit(1);
    }

    bzero((char *)&SERVER_ADDRESS, sizeof(SERVER_ADDRESS));
    SERVER_ADDRESS.sin_family = AF_INET;
    SERVER_ADDRESS.sin_addr.s_addr = INADDR_ANY;
    SERVER_ADDRESS.sin_port = htons(PORT);

    if (bind(SOCK_FD, (struct sockaddr *)&SERVER_ADDRESS, sizeof(SERVER_ADDRESS)) < 0)
    {
        // std::cerr << "An error while binding with sockets\n"<< std::endl;
        exit(1);
    }

    listen(SOCK_FD, 5);
    CLI_LEN = sizeof(CLIENT_ADDRESS);

    // std::cerr << "Listening To Port...." << PORT << std::endl;

    std::thread thread_pool[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++)
    {
        thread_pool[i] = std::thread(worker_thread, nullptr);
    }

    while (true)
    {
        NEW_SOCK_FD = accept(SOCK_FD, (struct sockaddr *)&CLIENT_ADDRESS, (socklen_t *)&CLI_LEN);
        if (NEW_SOCK_FD < 0)
        {
            // std::cout << "An error while accepting sockets\n"<< std::endl;
            exit(1);
        }

        std::lock_guard<std::mutex> lock(client_sockets_mutex);
        client_sockets.push(NEW_SOCK_FD);
    }

    close(SOCK_FD);
    return 0;
}
