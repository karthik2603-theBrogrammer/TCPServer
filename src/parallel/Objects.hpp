
#include <iostream>
#include <string>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sstream>
#include <queue>
#include <mutex>
#include <unordered_map>
std::unordered_map<std::string, std::string> KV_DATASTORE;
std::mutex KV_DATASTORE_MUTEX;
std::queue<int> client_sockets;
std::mutex client_sockets_mutex;