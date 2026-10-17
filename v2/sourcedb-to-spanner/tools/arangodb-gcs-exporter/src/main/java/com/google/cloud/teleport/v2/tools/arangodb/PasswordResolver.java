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

import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the ArangoDB password: {@code --passwordFile} (Hadoop URI, trimmed content), else the
 * environment variable named by {@code --passwordEnv}, else {@code --password} (logs a warning),
 * else the empty password (logs a warning).
 */
public final class PasswordResolver {

  private static final Logger LOG = LoggerFactory.getLogger(PasswordResolver.class);

  private PasswordResolver() {}

  /** Resolves with the process environment. */
  public static String resolve(ExporterOptions opts, Configuration conf) {
    return resolve(opts, conf, System::getenv);
  }

  static String resolve(ExporterOptions opts, Configuration conf, Function<String, String> env) {
    if (opts.passwordFile() != null) {
      LOG.info("Reading the ArangoDB password from {}", opts.passwordFile());
      return HadoopFiles.readString(opts.passwordFile(), conf).trim();
    }
    if (opts.passwordEnv() != null) {
      String fromEnv = env.apply(opts.passwordEnv());
      if (fromEnv != null) {
        LOG.info("Using the ArangoDB password from environment variable {}", opts.passwordEnv());
        return fromEnv;
      }
    }
    if (opts.password() != null) {
      LOG.warn(
          "Using the literal --password option. Prefer --passwordFile or --passwordEnv; command"
              + " line arguments are visible to other users and in job metadata.");
      return opts.password();
    }
    LOG.warn("No ArangoDB password configured; connecting with an empty password.");
    return "";
  }
}
