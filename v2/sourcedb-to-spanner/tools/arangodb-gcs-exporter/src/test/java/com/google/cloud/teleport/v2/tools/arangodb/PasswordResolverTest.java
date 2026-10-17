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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PasswordResolverTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static ExporterOptions opts(String... extra) {
    String[] base = {"--endpoints=h:1", "--output=/tmp/x"};
    String[] all = new String[base.length + extra.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(extra, 0, all, base.length, extra.length);
    return ExporterOptions.parse(all);
  }

  @Test
  public void fileWinsAndIsTrimmed() throws Exception {
    File f = tmp.newFile("pw");
    Files.write(f.toPath(), "  fromfile\n".getBytes(StandardCharsets.UTF_8));
    String pw =
        PasswordResolver.resolve(
            opts("--passwordFile=" + f.toURI(), "--password=literal"),
            new Configuration(),
            Map.of("ARANGO_PASSWORD", "fromenv")::get);
    assertThat(pw).isEqualTo("fromfile");
  }

  @Test
  public void envBeforeLiteral() {
    String pw =
        PasswordResolver.resolve(
            opts("--password=literal", "--passwordEnv=MY_PW"),
            new Configuration(),
            Map.of("MY_PW", "fromenv")::get);
    assertThat(pw).isEqualTo("fromenv");
  }

  @Test
  public void literalThenEmpty() {
    assertThat(
            PasswordResolver.resolve(
                opts("--password=literal"), new Configuration(), Map.<String, String>of()::get))
        .isEqualTo("literal");
    assertThat(PasswordResolver.resolve(opts(), new Configuration(), Map.<String, String>of()::get))
        .isEmpty();
    // An env var that is set but empty means "empty password" (e.g. root without password).
    assertThat(
            PasswordResolver.resolve(
                opts("--password=literal"),
                new Configuration(),
                Map.of("ARANGO_PASSWORD", "")::get))
        .isEmpty();
  }
}
