package utd.persistentDataStore.datastoreServer;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import utd.persistentDataStore.datastoreServer.commands.ServerCommand;
import utd.persistentDataStore.datastoreServer.servercommands.DeleteCommand;
import utd.persistentDataStore.datastoreServer.servercommands.DirectoryCommand;
import utd.persistentDataStore.datastoreServer.servercommands.ReadCommand;
import utd.persistentDataStore.datastoreServer.servercommands.WriteCommand;
import utd.persistentDataStore.utils.FileUtil;
import utd.persistentDataStore.utils.ServerException;
import utd.persistentDataStore.utils.StreamUtil;

public class DatastoreServer
{
    private static Logger logger = Logger.getLogger(DatastoreServer.class);
    
    static public final int port = 10023;
    
    public void startup() throws IOException
    {
        logger.debug("Starting Service at port " + port);
        FileUtil.createDirectory();
        
        ServerSocket serverSocket = new ServerSocket(port);
        
        InputStream inputStream = null;
        OutputStream outputStream = null;
        while (true) {
            try {
                logger.debug("Waiting for request");
                // The following accept() will block until a client connection
                // request is received at the configured port number
                Socket clientSocket = serverSocket.accept();
                
                logger.debug("Request received");
                
                inputStream = clientSocket.getInputStream();
                outputStream = clientSocket.getOutputStream();
                
                ServerCommand command = dispatchCommand(inputStream);
                logger.debug("Processing Request: " + command);
                command.setInputStream(inputStream);
                command.setOutputStream(outputStream);
                command.run();
                
                StreamUtil.closeSocket(inputStream);
                outputStream.close();
            }
            catch (ServerException ex) {
                String msg = ex.getMessage();
                logger.error("Exception while processing request. " + msg);
                StreamUtil.sendError(msg, outputStream);
                StreamUtil.closeSocket(inputStream);
                outputStream.close();
            }
            catch (Exception ex) {
                logger.error("Exception while processing request. " + ex.getMessage());
                ex.printStackTrace();
                StreamUtil.closeSocket(inputStream);
                outputStream.close();
            }
        }
    }
    
    private ServerCommand dispatchCommand(InputStream inputStream) throws ServerException
    {
        //implementing based on operation
        try {
            String commandType = StreamUtil.readLine(inputStream);
            System.out.println(commandType);
            if ("write".equalsIgnoreCase(commandType)) {
                WriteCommand command = new WriteCommand();
                return command;
            } else if ("read".equalsIgnoreCase(commandType)) {
                ReadCommand command = new ReadCommand();
                return command;
            } else if ("directory".equalsIgnoreCase(commandType)) {
                DirectoryCommand command = new DirectoryCommand();
                return command;
            } else if ("delete".equalsIgnoreCase(commandType)) {
                DeleteCommand command=new DeleteCommand();
                return command;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    public static void main(String args[])
    {
        DatastoreServer server = new DatastoreServer();
        try {
            server.startup();
        }
        catch (IOException ex) {
            logger.error("Unable to start server. " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
