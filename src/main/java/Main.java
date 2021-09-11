import com.brandontoner.duplicate.finder.DuplicateFinder;

import java.io.IOException;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws IOException {
        DuplicateFinder.builder()
                       .withKeepFolder(Paths.get("D:\\keep"))
                       .withDeleteFolder(Paths.get("D:\\delete"))
                       .withDeleteEmptyFolders(true)
                       .build()
                       .run();
    }
}
