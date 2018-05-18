To compile the project:

$ mvn clean compile assembly:single

To run the server:

$ cd server/target
$ java -jar server-*.jar -a 10.0.2.15

To run the client-proxy:

$ cd client_proxy
$ java -jar client_proxy-*.jar -a 10.0.2.15
 
