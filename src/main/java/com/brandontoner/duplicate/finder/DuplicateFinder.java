package com.brandontoner.duplicate.finder;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DuplicateFinder {
    private final List<Path> keepFolders;
    private final List<Path> deleteFolders;
    private final Predicate<Path> pathPredicate;
    private final boolean deleteEmptyFolders;

    private DuplicateFinder(Builder builder) {
        this.keepFolders = List.copyOf(builder.keepFolders);
        this.deleteFolders = List.copyOf(builder.deleteFolders);
        this.deleteEmptyFolders = builder.deleteEmptyFolders;
        this.pathPredicate = builder.pathPredicate;
    }

    public void run() throws IOException {
        List<Path> keepFiles = listFiles(keepFolders);
        Map<Optional<HashCode>, List<Path>> keepHashes =
                keepFiles.parallelStream().collect(Collectors.groupingByConcurrent(DuplicateFinder::hash));
        keepHashes.remove(Optional.<HashCode>empty());

        List<Path> deleteFiles = listFiles(deleteFolders);
        Map<Optional<HashCode>, List<Path>> deleteHashes =
                deleteFiles.parallelStream().collect(Collectors.groupingByConcurrent(DuplicateFinder::hash));
        deleteHashes.remove(Optional.<HashCode>empty());

        for (Map.Entry<Optional<HashCode>, List<Path>> entry : deleteHashes.entrySet()) {
            List<Path> toKeep = keepHashes.get(entry.getKey());
            final int startIndex;
            if (toKeep == null || toKeep.isEmpty()) {
                // no match in keep
                startIndex = 1;
            } else {
                startIndex = 0;
            }
            Collections.shuffle(entry.getValue());
            for (int i = startIndex; i < entry.getValue().size(); i++) {
                Path path = entry.getValue().get(i);
                System.err.println(path);
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        if (deleteEmptyFolders) {
            boolean deleted;
            do {
                deleted = false;

                List<Path> deleteDirs = deleteFolders.parallelStream()
                                                     .flatMap(DuplicateFinder::walk)
                                                     .filter(Files::isDirectory)
                                                     .toList();
                for (Path dir : deleteDirs) {
                    if (Files.list(dir).findAny().isEmpty()) {
                        // no children
                        try {
                            Files.delete(dir);
                            deleted = true;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.err.println("Not empty " + dir);
                    }
                }
            } while (deleted);
        }
    }

    private List<Path> listFiles(List<Path> keepFolders) {
        return keepFolders.parallelStream()
                          .flatMap(DuplicateFinder::walk)
                          .filter(Files::isRegularFile)
                          .filter(pathPredicate)
                          .toList();
    }

    private static Stream<Path> walk(Path path) {
        try {
            return Files.walk(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder {
        private final List<Path> keepFolders = new ArrayList<>();
        private final List<Path> deleteFolders = new ArrayList<>();
        private Predicate<Path> pathPredicate = ignored -> true;
        private boolean deleteEmptyFolders;

        public Builder withKeepFolder(Path... paths) {
            keepFolders.addAll(Arrays.asList(paths));
            return this;
        }

        public Builder withDeleteFolder(Path... paths) {
            deleteFolders.addAll(Arrays.asList(paths));
            return this;
        }

        public Builder withDeleteEmptyFolders(boolean v) {
            this.deleteEmptyFolders = v;
            return this;
        }

        public Builder withPathPredicate(Predicate<Path> predicate) {
            pathPredicate = pathPredicate.and(predicate);
            return this;
        }

        public DuplicateFinder build() {
            return new DuplicateFinder(this);
        }
    }

    private static Optional<HashCode> hash(Path path) {
        try {
            Hasher hasher = Hashing.sha512().newHasher();
            try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
                long size = fileChannel.size();
                for (long start = 0; start < size; ) {
                    long end = Math.min(start + Integer.MAX_VALUE, size);
                    ByteBuffer bb2 = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, end - start);
                    hasher.putBytes(bb2);
                    start = end;
                }
            }

            HashCode key = hasher.hash();
            System.err.println("Hash " + path + " " + key);
            return Optional.of(key);
        } catch (Exception e) {
            new RuntimeException("Error hashing " + path, e).printStackTrace();
        }
        return Optional.empty();
    }

    public static Builder builder() {
        return new Builder();
    }

}
