package utd.persistentDataStore.datastoreClient;

import java.io.IOException;


import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import utd.persistentDataStore.utils.StreamUtil;

public class DatastoreClientImpl implements DatastoreClient
{
    private static Logger logger = Logger.getLogger(DatastoreClientImpl.class);
    
    private InetAddress address;
    private int port;
    private InputStream instream;
    private OutputStream outStream;
    
    public DatastoreClientImpl(InetAddress address, int port)
    {
        this.address = address;
        this.port = port;
    }
    
    /* (non-Javadoc)
     * @see utd.persistentDataStore.datastoreClient.DatastoreClient#write(java.lang.String, byte[])
     */
    
    private void getSocketConnection() throws IOException {
        
        SocketAddress saddr = new InetSocketAddress(address, port);
        
        Socket socket = new Socket(address, port);
        try{
            socket.connect(saddr); // Connects to server
        }catch(Exception e)
        {}
        
        instream = socket.getInputStream();
        outStream = socket.getOutputStream();
        
    }
    
    //write
    @Override
    public void write(String name, byte data[]) throws ClientException
    {
        logger.debug("Executing Write Operation");
        try {
            
            getSocketConnection();
            StreamUtil.writeLine("write", outStream);
            StreamUtil.writeLine(name, outStream);
            StreamUtil.writeLine("" + data.length, outStream);
            StreamUtil.writeData(data, outStream);
            String reponseMsg = StreamUtil.readLine(instream);
            System.out.println("response is "+reponseMsg);
            if (!reponseMsg.equalsIgnoreCase("OK")) {
                
                throw new ClientException("Failed in write");
            }
            
        } catch (Exception ex) {
            throw new ClientException(ex.getMessage());
        } finally {
            StreamUtil.closeSocket(instream);
            try {
                outStream.close();
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }
        
    }
    
    /* (non-Javadoc)
     * @see utd.persistentDataStore.datastoreClient.DatastoreClient#read(java.lang.String)
     */
    
    //read
    @Override
    public byte[] read(String name) throws ClientException
    {
        logger.debug("Executing Read Operation");
        byte[] data = null;
        try {
            getSocketConnection();
            StreamUtil.writeLine("read", outStream);
            StreamUtil.writeLine(name, outStream);
            String reponseMsg = StreamUtil.readLine(instream);
            System.out.println("response is "+reponseMsg);
            if (reponseMsg.equalsIgnoreCase("ok")) {
                int byteDataSize = Integer.parseInt(StreamUtil.readLine(instream));
                data=StreamUtil.readData(byteDataSize, instream);
                
            } else {
                throw new ClientException("Failed reading");
            }
        }  catch (Exception ex) {
            throw new ClientException(ex.getMessage());
        } finally {
            StreamUtil.closeSocket(instream);
            try {
                outStream.close();
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }
        return data;
    }
    
    /* (non-Javadoc)
     * @see utd.persistentDataStore.datastoreClient.DatastoreClient#delete(java.lang.String)
     */
    
    //delete
    @Override
    public void delete(String name) throws ClientException
    {
        logger.debug("Executing Delete Operation");
        try {
            getSocketConnection();
            StreamUtil.writeLine("delete", outStream);
            StreamUtil.writeLine(name, outStream);
            String reponseMsg = StreamUtil.readLine(instream);
            System.out.println("response is "+reponseMsg);
            if (!reponseMsg.equalsIgnoreCase("ok")) {
                throw new ClientException("Failed deleting");
            }
        } catch (Exception ex) {
            throw new ClientException(ex.getMessage());
        } finally {
            StreamUtil.closeSocket(instream);
            try {
                outStream.close();
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }
    }
    
    /* (non-Javadoc)
     * @see utd.persistentDataStore.datastoreClient.DatastoreClient#directory()
     */
    
    //directory
    @Override
    public List<String> directory() throws ClientException
    {
        logger.debug("Executing Directory Operation");
        
        List<String> filenames=new ArrayList<String>();
        try {
            getSocketConnection();
            StreamUtil.writeLine("directory", outStream);
            String reponseMsg = StreamUtil.readLine(instream);
            if (reponseMsg.equalsIgnoreCase("ok")) {
                int tonumberOfFiles = Integer.parseInt(StreamUtil.readLine(instream));
                for (int i = 0; i < tonumberOfFiles; i++) {
                    String fileName=StreamUtil.readLine(instream);
                    filenames.add(fileName);
                }
                
            } else {
                throw new ClientException("Failed reading");
            }
        } catch (IOException ex) {
            throw new ClientException(ex.getMessage());
        } finally {
            StreamUtil.closeSocket(instream);
            try {
                outStream.close();
            } catch (Exception ex) {
                logger.debug(ex.getMessage());
            }
        }
        
        return filenames;
    }
    
}

