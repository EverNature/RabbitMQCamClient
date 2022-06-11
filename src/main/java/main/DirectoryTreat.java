package main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Stream;

public class DirectoryTreat extends RecursiveTask<List<Path>>{
	String directory;

	public DirectoryTreat(String directory) {
		super();
		this.directory = directory;
	}

	@Override
	protected List<Path> compute() {
		List<DirectoryTreat> innerDirectories = new ArrayList<>();
		List<Path> files = new ArrayList<>();
		
		try (Stream<Path> stream = Files.list(new File(directory).toPath())){
            stream.forEach(path -> {
                if (Files.isDirectory(path)) {
                	DirectoryTreat innerDirectory = new DirectoryTreat(path.toString());
                    innerDirectories.add(innerDirectory);
                    innerDirectory.fork();
                } else {
                    files.add(path);
                }
            });

            innerDirectories.stream().forEach(a -> a.join().forEach(b -> files.add(b)));

            return files;
        } catch (IOException e) {
            return new ArrayList<>();
        }
	}
}
