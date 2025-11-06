#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <httplib.h>

using httplib::Request;

using httplib::Response;

using std::cout;

void error(char *msg)
{
    perror(msg);
    exit(1);
}

int main(int argc, char *argv[])
{
      httplib::Server server;

    server.Get("/", [](const Request& req, Response& res) {
      cout<<"Get request received at / endpoint!!"<<"\n";
    res.set_content("Hello World!", "text/plain");
  });

  server.Get("/stop", [&](const Request& req, Response& res) {
    cout<<"Get request received at /stop endpoint"<<"\n";
    server.stop();
  });

  cout<<"Http server started and about to listen at localhost:2222 !!";

  server.listen("localhost", 2222);


       return 0; 
}