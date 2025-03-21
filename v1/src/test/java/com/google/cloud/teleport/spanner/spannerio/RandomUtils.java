/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

import java.util.Random;

/** Useful randomness related utilities. */
class RandomUtils {

  private static final char[] ALPHANUMERIC = "1234567890abcdefghijklmnopqrstuvwxyz".toCharArray();

  private RandomUtils() {}

  static String randomAlphaNumeric(int length) {
    Random random = new Random();
    char[] result = new char[length];
    for (int i = 0; i < length; i++) {
      result[i] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
    }
    return new String(result);
  }
}
