To redirect Mozilla Firefox brower to the client proxy:

Edit -> Preferences -> Advanced -> Network -> Settings ->
Manual Proxy Configurations ->

HTTP Proxy: 127.0.0.1
Port: 8000

-> OK

To compile the project:

$ mvn clean compile assembly:single

To run the server:

$ cd server/target
$ java -jar server-*.jar -a 10.0.2.15

To run the client-proxy:

$ cd client_proxy
$ java -jar client_proxy-*.jar -a 10.0.2.15
 
