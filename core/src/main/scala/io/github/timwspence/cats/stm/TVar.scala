package io.github.timwspence.cats.stm

import java.util.concurrent.atomic.AtomicReference

import io.github.timwspence.cats.stm.STM.internal._

/**
  * Transactional variable - a mutable memory location
  * that can be read or written to via `STM` actions.
  *
  * Analagous to `cats.effect.concurrent.Ref`.
  */
final class TVar[+A] private[stm] (
  private[stm] val id: Long,
  //Safe by construction
  @volatile private[stm] var value: Any,
  private[stm] val pending: AtomicReference[Map[TxId, Txn]]
) {

  /**
    * Get the current value as an
    * `STM` action.
    */
  def get: STM[A] = Get(this)

  /**
    * Set the current value as an
    * `STM` action.
    */
  def set[B >: A](b: B): STM[Unit] = modify(_ => b)

  /**
    * Modify the current value as an
    * `STM` action.
    */
  def modify[B >: A](f: A => B): STM[Unit] = Modify(this, f)

}

object TVar {

  def of[A](value: A): STM[TVar[A]] = Pure(new TVar(IdGen.incrementAndGet(), value, new AtomicReference(Map())))

}
