#include <iostream>
#include <cstring>
#include <map>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

std::map<std::string, std::string> KV_DATASTORE;

void handle_client(int client_socket)
{
  char buffer[1024];
  std::string command;

  while (1)
  {
    // Receive data from the client
    ssize_t bytes_received = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0)
    {
      break; // Connection closed or error
    }

    // Process received data
    buffer[bytes_received] = '\0';
    std::string received_data(buffer);

    // Split received data into lines
    std::size_t pos = 0;
    std::string delimiter = "\n";
    while ((pos = received_data.find(delimiter)) != std::string::npos)
    {
      std::string line = received_data.substr(0, pos);
      received_data.erase(0, pos + delimiter.length());

      if (line == "READ")
      {
        // std::cout << "READ GOT" << std::endl;
        // Extract the key
        pos = received_data.find(delimiter);
        if (pos != std::string::npos)
        {
          std::string key = received_data.substr(0, pos);
          received_data.erase(0, pos + delimiter.length());

          std::map<std::string, std::string>::iterator it = KV_DATASTORE.find(key);
          std::string response;
          if (it != KV_DATASTORE.end())
          {
            response = it->second + "\n";
            // response = "FIN\n";
          }
          else
          {
            response = "NULL\n";
          }

          send(client_socket, response.c_str(), response.length(), 0);
        }
        else
        {
          // in case the key is not found.
          // std::cout << "Key Not Found !" << std::endl;
        }
      }
      else if (line == "WRITE")
      {
        // std::cout << "WRITE GOT" << std::endl;

        // Extract the key
        pos = received_data.find(delimiter);
        if (pos != std::string::npos)
        {
          std::string key = received_data.substr(0, pos);
          received_data.erase(0, pos + delimiter.length());

          // Extract the value
          pos = received_data.find(delimiter);
          if (pos != std::string::npos)
          {
            std::string value_with_colon = received_data.substr(0, pos);
            received_data.erase(0, pos + delimiter.length());

            // Extract only the numeric part from the value
            size_t colon_pos = value_with_colon.find(":");
            if (colon_pos != std::string::npos)
            {
              std::string value = value_with_colon.substr(colon_pos + 1);

              // Store the key-value pair in the map
              KV_DATASTORE[key] = value;

              // std::string response = "Stored: " + key + " -> " + value + "\n";
              std::string response = "FIN\n";
              send(client_socket, response.c_str(), response.length(), 0);
            }
            else
            {
              // Handle incomplete command
              // std::cout << "Incomplete WRITE command: " << line << std::endl;
            }
          }
          else
          {
            // Handle incomplete command
            // std::cout << "Incomplete WRITE command: " << line << std::endl;
          }
        }
        else
        {
          // Handle incomplete command
          // std::cout << "Incomplete WRITE command: " << line << std::endl;
        }
      }
      else if (line == "COUNT")
      {
        // std::cout << "COUNT GOT" << std::endl;
        // Count the number of key-value pairs in the database
        int count = KV_DATASTORE.size();
        std::string response = std::to_string(count) + "\n";
        send(client_socket, response.c_str(), response.length(), 0);
      }
      else if (line == "DELETE")
{
    // std::cout << "DELETE GOT" << std::endl;

    // Extract the key from received_data
    pos = received_data.find(delimiter);
    if (pos != std::string::npos)
    {
        std::string key = received_data.substr(0, pos);
        received_data.erase(0, pos + delimiter.length());

        // Delete the key from the database
        size_t erased = KV_DATASTORE.erase(key);

        std::string response;
        if (erased > 0)
        {
            response = "FIN";
        }
        else
        {
            response = "NULL\n";
        }

        send(client_socket, response.c_str(), response.length(), 0);
    }
    else
    {
        // Handle incomplete command
        // std::cout << "Incomplete DELETE command: " << line << std::endl;
    }
}
      else if (line == "END")
      {
        // Close the client socket
        close(client_socket);
        return; // Exit the function
      }
      else
      {
        // Unsupported command
        const char *error_response = "Unsupported command";
        send(client_socket, error_response, strlen(error_response), 0);
      }
    }
  }
}

int main(int argc, char **argv)
{
  int server_socket, client_socket, portno;
  socklen_t clilen;
  struct sockaddr_in serv_addr, cli_addr;

  // Check command line arguments
  if (argc != 2)
  {
    std::cerr << "usage: " << argv[0] << " <port>\n";
    exit(1);
  }

  // Create a socket
  server_socket = socket(AF_INET, SOCK_STREAM, 0);
  if (server_socket < 0)
  {
    perror("Error opening socket");
    exit(1);
  }

  // Set up the server address structure
  memset((char *)&serv_addr, 0, sizeof(serv_addr));
  portno = atoi(argv[1]);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(portno);

  // Bind the socket to the address
  if (bind(server_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    perror("Error on binding");
    exit(1);
  }

  // Listen for incoming connections
  listen(server_socket, 5);
  clilen = sizeof(cli_addr);

  // std::cout << "Server listening on port " << portno << "...\n";

  // Accept incoming connections and handle them sequentially
  while (1)
  {
    client_socket = accept(server_socket, (struct sockaddr *)&cli_addr, &clilen);
    if (client_socket < 0)
    {
      perror("Error on accept");
      exit(1);
    }

    // Handle the client connection
    handle_client(client_socket);
  }

  // Close the server socket
  close(server_socket);

  return 0;
}
