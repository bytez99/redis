import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    ExecutorService executor;

    public RedisServer() throws IOException {
        executor = Executors.newCachedThreadPool();
        System.out.println("Listening on port " + port);

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            while (true) {
                clientSocket = serverSocket.accept();
                // Wait for new connection here
                System.out.println("New client connection from " + clientSocket.getRemoteSocketAddress());
                Socket finalClientSocket = clientSocket;

                executor.execute (() -> {
                    try {
                        handleClient(finalClientSocket);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            System.err.println("Error closing client socket: " + e.getMessage());
                        }
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Server socket error: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                    System.out.println("Server socket closed");
                } catch (IOException e) {
                    System.err.println("Error closing socket: " + e.getMessage());
                }
            }
            executor.shutdown();
            System.out.println("Exiting service shut down");
        }
    }


    public void handleClient(Socket socket) throws IOException {
        RESPParser parser = new RESPParser(socket.getInputStream());
        try {
            while (true) {
                try {
                    Object parsed = null;

                    try{
                        parsed = parser.parse();
                    } catch (IOException e) {
                        System.out.println("Client " + socket.getRemoteSocketAddress() + " disconnected or sent incomplete data: " + e.getMessage());
                        break;
                    } catch (Exception e) {
                        System.out.println("Parsing error from client " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
                        socket.getOutputStream().write(("-ERR protocol error: " + e.getMessage() + "\r\n").getBytes(StandardCharsets.UTF_8));
                        socket.getOutputStream().flush();
                        break;
                    }

                    if (parsed == null) {
                        System.out.println("Client " + socket.getRemoteSocketAddress() + " closed connection gracefully");
                        break;
                    }

                    System.out.println("Received from " + socket.getRemoteSocketAddress() + ": " + parsed.toString());

                    if (!(parsed instanceof List)){
                        System.out.println("Unexpected command format: " + parsed.getClass().getName());
                        socket.getOutputStream().write(("-ERR Invalid command format \r\n").getBytes(StandardCharsets.UTF_8));
                        socket.getOutputStream().flush();
                        continue;
                    }

                    List<Object> commands = (List<Object>) parsed;
                    if (commands.isEmpty()){
                        System.out.println("Received empty command from " + socket.getRemoteSocketAddress());
                        socket.getOutputStream().write(("-ERR Empty command \r\n").getBytes(StandardCharsets.UTF_8));
                        socket.getOutputStream().flush();
                        continue;
                    }

                    String commandName = commands.get(0).toString().toUpperCase();

                    switch (commandName){
                        case "HELLO" -> {
                            // More comprehensive RESP2 HELLO response
                            // This is an array of key-value pairs (server, version, proto, id, mode, role, modules)
                            String helloResponse = "*14\r\n" +
                                    "$6\r\nserver\r\n" +
                                    "$5\r\nMyRed\r\n" + // Changed from "redis" to "MyRed" for custom server
                                    "$7\r\nversion\r\n" +
                                    "$5\r\n1.0.0\r\n" + // Your server's version
                                    "$5\r\nproto\r\n" +
                                    ":2\r\n" +         // Indicate RESP2
                                    "$2\r\nid\r\n" +
                                    ":42\r\n" +        // A dummy client ID
                                    "$4\r\nmode\r\n" +
                                    "$10\r\nstandalone\r\n" +
                                    "$4\r\nrole\r\n" +
                                    "$6\r\nmaster\r\n" +
                                    "$7\r\nmodules\r\n" +
                                    "*0\r\n";          // Empty array for modules
                            socket.getOutputStream().write(helloResponse.getBytes(StandardCharsets.UTF_8));
                            System.out.println("Sent HELLO response."); // For debugging
                        }

                        case "PING" -> {
                            if (commands.size() == 1) {
                                socket.getOutputStream().write(("+PONG\r\n").getBytes(StandardCharsets.UTF_8));
                            } else {
                                String pingMessage = commands.get(1).toString();
                                byte[] messageBytes = pingMessage.getBytes(StandardCharsets.UTF_8);
                                String bulkStringResponse = "$" + messageBytes.length + "\r\n" + pingMessage + "\r\n";
                                socket.getOutputStream().write(bulkStringResponse.getBytes(StandardCharsets.UTF_8));
                            }

                            System.out.println("Sent PING response.");
                        }

                        case "ECHO" -> {
                            if (commands.size() < 2) {
                                socket.getOutputStream().write(("-ERR wrong number of arguments for 'echo' command\r\n").getBytes(StandardCharsets.UTF_8));
                            } else {
                                String messageToEcho = commands.get(1).toString();
                                byte[] messageBytes = messageToEcho.getBytes(StandardCharsets.UTF_8);
                                String echoResponse = "$" + messageBytes.length + "\r\n" + messageToEcho + "\r\n";
                                socket.getOutputStream().write(echoResponse.getBytes(StandardCharsets.UTF_8));
                            }
                            System.out.println("Sent ECHO response.");
                        }


                        case "CLIENT" -> {
                            socket.getOutputStream().write(("+OK\r\n").getBytes(StandardCharsets.UTF_8));
                        }

                        default -> {
                            String errorResponse = "-ERR unknown command: " + commandName + "\r\n";
                            socket.getOutputStream().write(errorResponse.getBytes(StandardCharsets.UTF_8));
                        }
                    }

                    socket.getOutputStream().flush();

                } catch (IOException e) {
                    System.out.println("Error: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }
}



