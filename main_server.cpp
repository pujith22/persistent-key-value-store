#include "server.h"

int main() {
    KeyValueServer server{"localhost", 2222};
    server.setupRoutes();
    server.start();
    return 0;
}
