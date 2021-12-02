package com.brandontoner.duplicate.finder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DuplicateFinder {
    private final List<Path> keepFolders;
    private final List<Path> deleteFolders;
    private final Predicate<Path> pathPredicate;
    private final ThrowingConsumer<Path, ? extends IOException> deleteFunction;
    private final boolean deleteEmptyFolders;

    private DuplicateFinder(Builder builder) {
        this.keepFolders = List.copyOf(builder.keepFolders);
        this.deleteFolders = List.copyOf(builder.deleteFolders);
        this.deleteEmptyFolders = builder.deleteEmptyFolders;
        this.pathPredicate = builder.pathPredicate;
        this.deleteFunction = Objects.requireNonNull(builder.deleteFunction);
    }

    public void run() throws IOException {
        var sizeToKeepFilesTask = ForkJoinPool.commonPool()
                                              .submit(() -> this.listFiles(keepFolders)
                                                                .parallelStream()
                                                                .collect(groupBySize()));
        var sizeToDeleteFilesTask = ForkJoinPool.commonPool()
                                                .submit(() -> this.listFiles(deleteFolders)
                                                                  .parallelStream()
                                                                  .collect(groupBySize()));

        ImmutableSortedMap<Long, ImmutableList<Path>> sizeToKeepFiles = sizeToKeepFilesTask.join();
        ImmutableSortedMap<Long, ImmutableList<Path>> sizeToDeleteFiles = sizeToDeleteFilesTask.join();

        sizeToDeleteFiles.entrySet().parallelStream().forEach(entry -> {
            List<Path> keepPaths = sizeToKeepFiles.getOrDefault(entry.getKey(), ImmutableList.of());
            List<Path> deletePaths = entry.getValue();

            if (deletePaths.size() + keepPaths.size() <= 1) {
                return;
            }
            Set<HashCode> keepHashes = keepPaths.parallelStream()
                                                .map(DuplicateFinder::hash)
                                                .filter(Optional::isPresent)
                                                .map(Optional::get)
                                                .collect(Collectors.toSet());

            Set<HashCode> deleteHashes = Collections.newSetFromMap(new ConcurrentHashMap<>());

            deletePaths.parallelStream().forEach(path -> {
                Optional<HashCode> optionalHashCode = hash(path);
                if (optionalHashCode.isEmpty()) {
                    return;
                }
                HashCode hashCode = optionalHashCode.get();
                if (keepHashes.contains(hashCode) || !deleteHashes.add(hashCode)) {
                    delete(path);
                }
            });
        });

        if (deleteEmptyFolders) {
            for (Path deleteFolder : deleteFolders) {
                deleteIfEmpty(deleteFolder.toFile());
            }

            for (Path keepFolder : keepFolders) {
                deleteIfEmpty(keepFolder.toFile());
            }
        }
    }

    private void delete(Path existing) {
        System.err.println("Deleting " + existing);
        try {
            deleteFunction.accept(existing);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Collector<Path, Object, ImmutableSortedMap<Long, ImmutableList<Path>>> groupBySize() {
        return Collectors.collectingAndThen(Collectors.groupingBy(DuplicateFinder::size), DuplicateFinder::immutablize);
    }

    private static <K extends Comparable<K>, V> ImmutableSortedMap<K, ImmutableList<V>> immutablize(Map<K, ? extends List<V>> r) {
        return r.entrySet()
                .stream()
                .collect(ImmutableSortedMap.toImmutableSortedMap(K::compareTo,
                                                                 Map.Entry::getKey,
                                                                 e -> ImmutableList.copyOf(e.getValue())));
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Collection<Path> listFiles(Collection<? extends Path> keepFolders) {
        return streamFiles(keepFolders)
                          .collect(Collectors.toUnmodifiableSet());
    }

    private Stream<Path> streamFiles(Collection<? extends Path> keepFolders) {
        System.err.println("Listing files in " + keepFolders);
        return keepFolders.parallelStream()
                .flatMap(DuplicateFinder::walk)
                .filter(Files::isRegularFile)
                .filter(pathPredicate);
    }

    private static boolean deleteIfEmpty(File path) {
        boolean empty = true;
        for (File file : path.listFiles()) {
            if (file.isDirectory()) {
                if (!deleteIfEmpty(file)) {
                    empty = false;
                }
            } else {
                empty = false;
            }
        }
        if (empty) {
            return path.delete();
        }
        return false;
    }

    private static Stream<Path> walk(Path path) {
        try {
            if (Files.isRegularFile(path)) {
                return Stream.of(path);
            }
            if (Files.isDirectory(path)) {
                return Files.list(path).flatMap(DuplicateFinder::walk);
            }
            return Stream.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder {
        private final List<Path> keepFolders = new ArrayList<>();
        private final List<Path> deleteFolders = new ArrayList<>();
        private Predicate<Path> pathPredicate = ignored -> true;
        private boolean deleteEmptyFolders;
        private ThrowingConsumer<Path, ? extends IOException> deleteFunction = Files::delete;

        public Builder withKeepFolder(Path... paths) {
            keepFolders.addAll(Arrays.asList(paths));
            return this;
        }

        public Builder withKeepFolder(String... paths) {
            return withKeepFolder(Arrays.stream(paths).map(Path::of).toArray(Path[]::new));
        }

        public Builder withDeleteFolder(Path... paths) {
            deleteFolders.addAll(Arrays.asList(paths));
            return this;
        }

        public Builder withDeleteFolder(String... paths) {
            return withDeleteFolder(Arrays.stream(paths).map(Path::of).toArray(Path[]::new));
        }

        public Builder withDeleteEmptyFolders(boolean v) {
            this.deleteEmptyFolders = v;
            return this;
        }

        public Builder withPathPredicate(Predicate<Path> predicate) {
            pathPredicate = pathPredicate.and(predicate);
            return this;
        }

        public Builder withDeleteFunction(ThrowingConsumer<Path, ? extends IOException> c) {
            this.deleteFunction = c;
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
        } catch (Throwable e) {
            new RuntimeException("Error hashing " + path, e).printStackTrace();
        }
        return Optional.empty();
    }

    public static Builder builder() {
        return new Builder();
    }

}
