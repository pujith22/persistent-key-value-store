#include <string.h>

#include <httplib.h>

using httplib::Request;

using httplib::Response;

using std::cout;

using std::string;


const int DEFAULT_PORTNO = 2222;

const string server_location = "localhost";

void error(char *msg)
{
    perror(msg);
    exit(1);
}

const string home_page_html = "<html> <h1 style= \"color: brown;\"> Welcome to Persistent Key Value Server!!</h1>"

"<h3 style= \"color: green;\"> Author: Pujith Sai Kumar Korepara </h3>"

"<h3 style= \"color: green;\"> ID: 25M0787 </h3>"

"<table> <tr style= \"color: #191970;\"> <th> Route </th> <th> Description </th> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> GET /get_key/:key  </td>  <td> Returns value associated with key if present else will throws an error response. </td> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> POST /bulk_query </td> <td> Tries to retrieve values for all queries in this post request, ignoring keys that are not present and passing the corresponding information in the response. </td> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> POST /insert/:key/:value </td> <td> Inserts key-value pair provided in to the database if it doesn't exit already, otherwise throws an error response. </td> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> PATCH /bulk_update </td> <td>Process all insertions and update queries ignoring errors and sending response accordingly (Basically a way of commiting partial updates) </td> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> DELETE /delete_key/:key </td> <td> Deletes key from the key-value store if it exists, otherwise throws an error response. </td> </tr>"

"<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> PUT /update_key/:key/:value </td> <td> Updates value corresponsing to 'key' with 'value' if 'key' exists and throws and error response otherwise. </td> </tr>"
"</table> </html>";

void request_logger(const httplib::Request& request)
{
  string requested_path = request.path;
  string http_method = request.method;
  cout<< "\n"<< http_method <<" request received at "<<requested_path<<" endpoint. "<<"\n";
}

void response_logger(const httplib::Response& response)
{
  int status = response.status;
  string reason = response.reason;
  string body = response.body;
  string content_type = response.file_content_content_type_;
  string version = response.version;

  cout<<"\n{\n"<<"Status: "<<status<<"\n Reason: "<<reason<<"\n Version: "<<version<<"\n Content Type: "<<content_type<<"\n Body: "<<body<<"\n}\n\n";
  
}

void home_page_handler(const httplib::Request& request, httplib::Response& response)
{
    request_logger(request);

    response.set_content(home_page_html, "text/html");
    
    response_logger(response);
}


void get_key_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  cout<<"\nRecieved a request to get value of key: "<< request.path_params.at("key_id") <<"\n";

  auto id = request.path_params.at("key_id");

  cout<<stoi(id)+100<<"\n";

  response.set_content("\nThanks for querying for the key: " + id +" The Server will be soon available to serve your request!! We appreciate your Patience\n", "text/plain");

  response_logger(response);
}

void bulk_query_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  response.set_content("The server is under maintainence, Thanks for your patience!!", "text/plain");

  response_logger(response);
}

void insertion_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  response.set_content(request.body, "text/json");

  response_logger(response);
}

void bulk_update_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  response.set_content(request.body, "text/json");

  response_logger(response);
}

void deletion_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  response.set_content(request.body, "text/json");

  response_logger(response);
}

void updation_handler(const httplib::Request& request, httplib::Response& response)
{
  request_logger(request);

  response.set_content(request.body, "text/json");

  response_logger(response);
}

int main(int argc, char *argv[])
{
    httplib::Server server;

    // Both routes are for the same purpose

    server.Get("/", home_page_handler);

    server.Get("/home", home_page_handler);


    server.Get("/get_key/:key_id", get_key_handler);

    server.Post("/bulk_query", bulk_query_handler);

    server.Post("/insert/:key/:value", insertion_handler);

    server.Patch("/bulk_update", bulk_update_handler);

    server.Delete("/delete_key/:key", deletion_handler);

    server.Put("/update_key/:key/:value", updation_handler);

    /* Need to look it to this, so that one request should stop the entire server. Or entirely needed to remove it if necesssary*/
    server.Get("/stop", [&](const Request& request, Response& response) {
    request_logger(request);
    server.stop();
    response_logger(response);
  });

  cout<< "Http server started and about to listen @ " + server_location + ":" <<DEFAULT_PORTNO<<" !!!";

  server.listen(server_location, DEFAULT_PORTNO);

  return 0; 
}