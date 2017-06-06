package utd.persistentDataStore.datastoreServer.servercommands;

import java.io.IOException;

import utd.persistentDataStore.datastoreServer.commands.ServerCommand;
import utd.persistentDataStore.utils.FileUtil;
import utd.persistentDataStore.utils.ServerException;
import utd.persistentDataStore.utils.StreamUtil;

public class DeleteCommand extends ServerCommand {

	private String fileName ;

	@Override
	public void run() throws IOException, ServerException {
		String fileName=StreamUtil.readLine(inputStream);
		if(FileUtil.deleteData(fileName)){
			StreamUtil.writeLine("ok", outputStream);	
		}
		else {
			throw new ServerException("Could not delete");
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
