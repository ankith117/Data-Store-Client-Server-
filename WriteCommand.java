package utd.persistentDataStore.datastoreServer.servercommands;

import java.io.IOException;

import java.io.InputStream;

import utd.persistentDataStore.datastoreServer.commands.ServerCommand;

import utd.persistentDataStore.utils.FileUtil;
import utd.persistentDataStore.utils.ServerException;
import utd.persistentDataStore.utils.StreamUtil;

public class WriteCommand extends ServerCommand {
	

	@Override
	public void run() throws IOException, ServerException {
		String fileName =StreamUtil.readLine(inputStream);
		int length=Integer.parseInt(StreamUtil.readLine(inputStream));
		byte[] data  = StreamUtil.readData(length, inputStream);
		FileUtil.writeData(fileName, data);
		sendOK();
	}
}
