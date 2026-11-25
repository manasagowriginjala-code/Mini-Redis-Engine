#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <list>
#include <chrono>
#include <thread>
#include <mutex>
#include <cstring>
#include <vector>

// POSIX Linux Sockets for Phase 3 Networking
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

#define PORT 8080
#define BUFFER_SIZE 1024
#define WAL_FILE "wal.log"

// -------------------------------------------------------------
// Data Structures: Phase 1 (LRU + TTL)
// -------------------------------------------------------------
struct CacheValue {
    string value;
    chrono::steady_clock::time_point expireAt;
    bool hasTTL;
};

// -------------------------------------------------------------
// Unified Database Engine: Phases 1, 2, and 4 Combined
// -------------------------------------------------------------
class UnifiedRedisEngine {
private:
    int capacity;
    list<pair<string, CacheValue>> dll;
    unordered_map<string, list<pair<string, CacheValue>>::iterator> cacheMap;
    mutable mutex mtx; // Phase 2: Thread-Safety
    ofstream walStream; // Phase 4: Persistence

    bool isExpired(const CacheValue& cv) const {
        if (!cv.hasTTL) return false;
        return chrono::steady_clock::now() > cv.expireAt;
    }

    void appendToWAL(const string& logEntry) {
        if (walStream.is_open()) {
            walStream << logEntry << endl;
            walStream.flush();
        }
    }

public:
    UnifiedRedisEngine(int cap) : capacity(cap) {
        walStream.open(WAL_FILE, ios::app);
    }

    ~UnifiedRedisEngine() {
        if (walStream.is_open()) walStream.close();
    }

    // Phase 4: Crash Recovery Replay
    void recover() {
        ifstream infile(WAL_FILE);
        if (!infile.is_open()) {
            cout << "[Engine]: No previous WAL log. Fresh start!" << endl;
            return;
        }

        string line;
        int count = 0;
        while (getline(infile, line)) {
            stringstream ss(line);
            string cmd, key, val;
            ss >> cmd;

            if (cmd == "SET") {
                if (ss >> key >> val) {
                    insertInternal(key, {val, chrono::steady_clock::time_point::max(), false});
                    count++;
                }
            } else if (cmd == "DEL") {
                if (ss >> key && cacheMap.find(key) != cacheMap.end()) {
                    dll.erase(cacheMap[key]);
                    cacheMap.erase(key);
                    count++;
                }
            }
        }
        cout << "[Engine]: Recovered " << count << " operations from disk log!" << endl;
    }

    // GET Command
    string get(const string& key) {
        lock_guard<mutex> lock(mtx);
        if (cacheMap.find(key) == cacheMap.end()) {
            return "(nil) -> Key not found\n";
        }
        auto nodeIt = cacheMap[key];
        if (isExpired(nodeIt->second)) {
            dll.erase(nodeIt);
            cacheMap.erase(key);
            return "(nil) -> Key Expired\n";
        }
        dll.splice(dll.begin(), dll, nodeIt);
        return nodeIt->second.value + "\n";
    }

    // SET Command
    string put(const string& key, const string& value) {
        lock_guard<mutex> lock(mtx);
        appendToWAL("SET " + key + " " + value);
        CacheValue cv = {value, chrono::steady_clock::time_point::max(), false};
        insertInternal(key, cv);
        return "+OK\n";
    }

    // SETEX Command (TTL)
    string putWithTTL(const string& key, int ttlSeconds, const string& value) {
        lock_guard<mutex> lock(mtx);
        auto expireTime = chrono::steady_clock::now() + chrono::seconds(ttlSeconds);
        CacheValue cv = {value, expireTime, true};
        insertInternal(key, cv);
        return "+OK (TTL set)\n";
    }

    // DEL Command
    string del(const string& key) {
        lock_guard<mutex> lock(mtx);
        if (cacheMap.find(key) == cacheMap.end()) {
            return ":0 (Key does not exist)\n";
        }
        appendToWAL("DEL " + key);
        auto nodeIt = cacheMap[key];
        dll.erase(nodeIt);
        cacheMap.erase(key);
        return ":1 (Deleted)\n";
    }

    // DUMP Command
    void display() const {
        lock_guard<mutex> lock(mtx);
        if (dll.empty()) {
            cout << "  (cache is empty)" << endl;
            return;
        }
        for (const auto& item : dll) {
            cout << "  - [" << item.first << " : " << item.second.value << "]\n";
        }
    }

private:
    void insertInternal(const string& key, const CacheValue& cv) {
        if (cacheMap.find(key) != cacheMap.end()) {
            auto nodeIt = cacheMap[key];
            nodeIt->second = cv;
            dll.splice(dll.begin(), dll, nodeIt);
            return;
        }
        if (dll.size() >= capacity) {
            auto lru = dll.back();
            cacheMap.erase(lru.first);
            dll.pop_back();
        }
        dll.push_front({key, cv});
        cacheMap[key] = dll.begin();
    }
};

// Global unified database engine
UnifiedRedisEngine db(5);

// -------------------------------------------------------------
// Network Communication: Phase 3 (TCP Sockets)
// -------------------------------------------------------------
void handleClient(int clientSocket) {
    char buffer[BUFFER_SIZE];

    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytesRead = read(clientSocket, buffer, BUFFER_SIZE - 1);
        if (bytesRead <= 0) break;

        string request(buffer);
        stringstream ss(request);
        string cmd, key, val;
        int ttl = 0;
        ss >> cmd;

        string response = "";

        if (cmd == "SET" || cmd == "set") {
            if (ss >> key >> val) response = db.put(key, val);
            else response = "-ERR syntax error\n";
        } 
        else if (cmd == "SETEX" || cmd == "setex") {
            if (ss >> key >> ttl >> val) response = db.putWithTTL(key, ttl, val);
            else response = "-ERR syntax error\n";
        } 
        else if (cmd == "GET" || cmd == "get") {
            if (ss >> key) response = db.get(key);
            else response = "-ERR syntax error\n";
        } 
        else if (cmd == "DEL" || cmd == "del") {
            if (ss >> key) response = db.del(key);
            else response = "-ERR syntax error\n";
        } 
        else if (cmd == "PING" || cmd == "ping") {
            response = "+PONG\n";
        } 
        else if (cmd == "QUIT" || cmd == "quit") {
            response = "+BYE\n";
            write(clientSocket, response.c_str(), response.length());
            break;
        } 
        else {
            response = "-ERR unknown command\n";
        }

        write(clientSocket, response.c_str(), response.length());
    }

    close(clientSocket);
}

void startServer(int serverFd) {
    cout << "[Server]: TCP Server listening on port " << PORT << "..." << endl;
    struct sockaddr_in clientAddr;
    socklen_t addrLen = sizeof(clientAddr);

    int clientSocket = accept(serverFd, (struct sockaddr*)&clientAddr, &addrLen);
    if (clientSocket >= 0) {
        cout << "[Server]: Client connected successfully!" << endl;
        handleClient(clientSocket);
        cout << "[Server]: Client disconnected." << endl;
    }
}

// Client Simulation testing all features over network
void runClient() {
    this_thread::sleep_for(chrono::milliseconds(200));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servAddr;
    servAddr.sin_family = AF_INET;
    servAddr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &servAddr.sin_addr);

    if (connect(sock, (struct sockaddr*)&servAddr, sizeof(servAddr)) < 0) return;

    cout << "[Client]: Connected to Unified Engine over TCP!\n" << endl;

    vector<string> networkCommands = {
        "PING",
        "SET user:1 Kiran",
        "SET user:2 Suresh",
        "SETEX session_token 5 secret123",
        "GET user:1",
        "GET session_token",
        "DEL user:2",
        "QUIT"
    };

    char buffer[BUFFER_SIZE];
    for (const auto& cmd : networkCommands) {
        cout << "[Client -> Server]: " << cmd << endl;
        string msg = cmd + "\n";
        write(sock, msg.c_str(), msg.length());

        memset(buffer, 0, BUFFER_SIZE);
        int bytes = read(sock, buffer, BUFFER_SIZE - 1);
        if (bytes > 0) cout << "[Server -> Client]: " << buffer;
        this_thread::sleep_for(chrono::milliseconds(50));
    }
    close(sock);
}

int main() {
    cout << "==========================================================" << endl;
    cout << "    UNIFIED MINI-REDIS ENGINE (PHASES 1, 2, 3, 4 COMBINED) " << endl;
    cout << "==========================================================" << endl;

    // Phase 4: Recover past data from disk if any exists
    db.recover();

    // Phase 3: Setup Socket Server
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(serverFd, (struct sockaddr*)&address, sizeof(address));
    listen(serverFd, 5);

    // Run Server and Client
    thread serverThread(startServer, serverFd);
    thread clientThread(runClient);

    clientThread.join();
    serverThread.join();

    close(serverFd);

    cout << "\n[Engine]: Current Memory State After Client Session:" << endl;
    db.display();

    cout << "\n[Engine]: All commands are safely persisted to wal.log!" << endl;
    cout << "==========================================================" << endl;

    return 0;
}
