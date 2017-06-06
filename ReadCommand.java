package utd.persistentDataStore.datastoreServer.servercommands;

import java.io.IOException;

import utd.persistentDataStore.datastoreServer.commands.ServerCommand;
import utd.persistentDataStore.utils.FileUtil;
import utd.persistentDataStore.utils.ServerException;
import utd.persistentDataStore.utils.StreamUtil;

public class ReadCommand extends ServerCommand {
	private String fileName;

	@Override
	public void run() throws IOException, ServerException {
		String fileName=StreamUtil.readLine(inputStream);
		byte[] storeData=FileUtil.readData(fileName);
		StreamUtil.writeLine("ok", outputStream);
		System.out.println("length of data stored " +storeData.length);
		StreamUtil.writeLine(""+storeData.length, outputStream);
		StreamUtil.writeData(storeData, outputStream);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
