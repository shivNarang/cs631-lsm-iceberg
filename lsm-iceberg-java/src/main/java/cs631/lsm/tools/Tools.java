package cs631.lsm.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

final class Tools {
  private Tools() {}

  static void deleteRecursively(Path p) throws IOException {
    if (!Files.exists(p)) return;
    try (Stream<Path> s = Files.walk(p)) {
      for (Path x : s.sorted(Comparator.reverseOrder()).toList()) Files.delete(x);
    }
  }

  static long count(Iterable<?> it) {
    long n = 0;
    for (Object ignored : it) n++;
    return n;
  }

  static long dirBytes(Path p) throws IOException {
    if (!Files.exists(p)) return 0;
    try (Stream<Path> s = Files.walk(p)) {
      return s.filter(Files::isRegularFile).filter(x -> !x.getFileName().toString().endsWith(".crc"))
          .mapToLong(x -> x.toFile().length()).sum();
    }
  }
}
