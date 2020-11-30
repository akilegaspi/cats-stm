/*
 * Copyright 2020 TimWSpence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.timwspence.cats.stm

import cats.effect.IO
import munit.CatsEffectSuite

class TSemaphoreTest extends CatsEffectSuite {

  val stm = STM[IO]().unsafeRunSync()
  import stm._

  test("Acquire decrements the number of permits") {
    val prog: Txn[Long] = for {
      tsem  <- TSemaphore.make(1)
      _     <- tsem.acquire
      value <- tsem.available
    } yield value

    for (value <- stm.commit(prog)) yield assertEquals(value, 0L)
  }

  test("Release increments the number of permits") {
    val prog: Txn[Long] = for {
      tsem  <- TSemaphore.make(0)
      _     <- tsem.release
      value <- tsem.available
    } yield value

    for (value <- stm.commit(prog)) yield assertEquals(value, 1L)
  }

}
