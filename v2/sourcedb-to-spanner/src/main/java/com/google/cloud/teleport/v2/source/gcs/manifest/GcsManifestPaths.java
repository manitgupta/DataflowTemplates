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
package com.google.cloud.teleport.v2.source.gcs.manifest;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;

/** Resolves file paths listed in a manifest relative to the manifest's directory. */
public final class GcsManifestPaths {

  private GcsManifestPaths() {}

  /**
   * Resolves a path from the manifest.
   *
   * @param manifestPath URI of the manifest.
   * @param relativeOrAbsolute a path (or glob) from the manifest. Values containing {@code ://} or
   *     starting with {@code /} are absolute and returned unchanged; all others are resolved
   *     against the manifest's directory.
   * @return the resolved URI.
   */
  public static String resolve(String manifestPath, String relativeOrAbsolute) {
    if (relativeOrAbsolute.contains("://") || relativeOrAbsolute.startsWith("/")) {
      return relativeOrAbsolute;
    }
    return FileSystems.matchNewResource(manifestPath, /* isDirectory= */ false)
        .getCurrentDirectory()
        .resolve(relativeOrAbsolute, StandardResolveOptions.RESOLVE_FILE)
        .toString();
  }
}
