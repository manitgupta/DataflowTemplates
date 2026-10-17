/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.tools.arangodb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/** Small helpers over the Hadoop {@link FileSystem} API (works for gs://, hdfs://, file://). */
public final class HadoopFiles {

  private HadoopFiles() {}

  /** Reads a whole file as UTF-8. */
  public static String readString(String uri, Configuration conf) {
    Path path = new Path(uri);
    try {
      FileSystem fs = path.getFileSystem(conf);
      try (InputStream in = fs.open(path)) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        in.transferTo(out);
        return out.toString(StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read " + uri, e);
    }
  }

  /**
   * Writes a UTF-8 file. With {@code overwrite=false} an existing file makes this fail.
   *
   * @throws UncheckedIOException on I/O errors, including an existing file without overwrite
   */
  public static void writeString(
      String uri, String content, Configuration conf, boolean overwrite) {
    Path path = new Path(uri);
    try {
      FileSystem fs = path.getFileSystem(conf);
      try (OutputStream out = fs.create(path, overwrite)) {
        out.write(content.getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot write " + uri, e);
    }
  }

  /** Whether a file or directory exists. */
  public static boolean exists(String uri, Configuration conf) {
    Path path = new Path(uri);
    try {
      return path.getFileSystem(conf).exists(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot check " + uri, e);
    }
  }

  /**
   * Recursively lists data files below {@code dirUri} whose name ends with {@code suffix}, skipping
   * hidden files and files below hidden directories (any path component below {@code dirUri}
   * starting with {@code _} or {@code .}, e.g. {@code _temporary/} or {@code .spark-staging-*}),
   * which matches the files Spark itself reads back. Returns fully qualified paths.
   */
  public static List<Path> listFiles(String dirUri, String suffix, Configuration conf) {
    Path dir = new Path(dirUri);
    List<Path> result = new ArrayList<>();
    try {
      FileSystem fs = dir.getFileSystem(conf);
      if (!fs.exists(dir)) {
        return result;
      }
      String dirPath = fs.makeQualified(dir).toUri().getPath();
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, true);
      while (it.hasNext()) {
        LocatedFileStatus status = it.next();
        Path qualified = fs.makeQualified(status.getPath());
        if (status.isFile()
            && qualified.getName().endsWith(suffix)
            && !isHidden(relativePath(dirPath, qualified.toUri().getPath()))) {
          result.add(qualified);
        }
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot list " + dirUri, e);
    }
  }

  /** Path of {@code filePath} relative to {@code dirPath} (both URI paths without scheme). */
  static String relativePath(String dirPath, String filePath) {
    if (filePath.startsWith(dirPath)) {
      return filePath.substring(dirPath.length());
    }
    return filePath;
  }

  /** Whether any component of a relative path starts with {@code _} or {@code .}. */
  static boolean isHidden(String relativePath) {
    for (String component : relativePath.split("/")) {
      if (component.startsWith("_") || component.startsWith(".")) {
        return true;
      }
    }
    return false;
  }

  /** Fully qualifies a path (adds scheme and authority) with its file system. */
  public static Path qualify(String uri, Configuration conf) {
    Path path = new Path(uri);
    try {
      return path.getFileSystem(conf).makeQualified(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot qualify " + uri, e);
    }
  }
}
