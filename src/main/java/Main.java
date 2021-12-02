import com.brandontoner.duplicate.finder.DuplicateFinder;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Set;

public class Main {
    private static final Set<String> EXTENSIONS = Set.of(".jpeg", ".jpg");

    public static void main(String[] args) throws IOException {
        DuplicateFinder.builder()
                       .withKeepFolder(Paths.get("D:\\Users\\brand\\Pictures\\iCloud Photos\\Photos"))
                       .withDeleteFolder(Paths.get("D:\\Users\\brand\\Pictures\\New folder"))
                       // .withPathPredicate(path -> EXTENSIONS.contains(getExtension(path)))
                       .withDeleteEmptyFolders(true)
                       .build()
                       .run();
    }

    private static String getExtension(Path path) {
        String pathStr = path.toAbsolutePath().toString();
        return pathStr.substring(pathStr.lastIndexOf('.')).toLowerCase();
    }
}
