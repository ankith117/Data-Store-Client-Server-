package utd.persistentDataStore.datastoreServer.servercommands;

import java.io.IOException;
import java.util.List;

import utd.persistentDataStore.datastoreServer.commands.ServerCommand;
import utd.persistentDataStore.utils.FileUtil;
import utd.persistentDataStore.utils.ServerException;
import utd.persistentDataStore.utils.StreamUtil;

public class DirectoryCommand extends ServerCommand {

	@Override
	public void run() throws IOException, ServerException {
		List<String> files=FileUtil.directory();
		StreamUtil.writeLine("ok", outputStream);
		StreamUtil.writeLine(new Integer(files.size()).toString(), outputStream);
		for (String file : files) {
			StreamUtil.writeLine(file, outputStream);
		}
		
		
	}

}
