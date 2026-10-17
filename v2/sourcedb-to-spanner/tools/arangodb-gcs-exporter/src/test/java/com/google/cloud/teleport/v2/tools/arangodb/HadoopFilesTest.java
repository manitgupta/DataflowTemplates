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

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopFilesTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static void touch(File root, String relative) throws Exception {
    File f = new File(root, relative);
    f.getParentFile().mkdirs();
    Files.write(f.toPath(), new byte[] {1});
  }

  @Test
  public void listFiles_skipsHiddenFilesAndHiddenDirectories() throws Exception {
    File root = tmp.newFolder("tables", "t");
    touch(root, "b00000/part-00000.avro");
    touch(root, "b00001/part-00001.avro");
    touch(root, "b00001/_SUCCESS");
    touch(root, "b00001/.part-00001.avro.crc");
    touch(root, "b00001/_tmp.avro");
    touch(root, "b00002/_temporary/0/_temporary/attempt_1/part-00009.avro");
    touch(root, "b00002/.spark-staging-1/part-00008.avro");
    touch(root, "b00002/x.json");

    List<String> names =
        HadoopFiles.listFiles(root.toURI().toString(), ".avro", new Configuration()).stream()
            .map(Path::toString)
            .map(s -> s.substring(s.indexOf("/t/") + 3))
            .sorted()
            .collect(Collectors.toList());

    assertThat(names).containsExactly("b00000/part-00000.avro", "b00001/part-00001.avro");
  }

  @Test
  public void isHidden_checksEveryComponent() {
    assertThat(HadoopFiles.isHidden("/b00000/part-0.avro")).isFalse();
    assertThat(HadoopFiles.isHidden("/_temporary/0/part-0.avro")).isTrue();
    assertThat(HadoopFiles.isHidden("/b0/.spark-staging-1/part-0.avro")).isTrue();
    assertThat(HadoopFiles.isHidden("/b0/_part.avro")).isTrue();
  }

  @Test
  public void listFiles_missingDirectoryIsEmpty() {
    assertThat(
            HadoopFiles.listFiles(
                new File(tmp.getRoot(), "nope").toURI().toString(), ".avro", new Configuration()))
        .isEmpty();
  }
}
