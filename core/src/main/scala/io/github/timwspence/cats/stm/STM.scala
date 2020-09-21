package io.github.timwspence.cats.stm

import java.util.concurrent.atomic.AtomicLong

import cats.implicits._
import cats.{Monad, Monoid, MonoidK}
import cats.effect.Concurrent
import cats.effect.implicits._
import cats.effect.concurrent.Deferred

import scala.annotation.tailrec
import scala.compat.java8.FunctionConverters._

import io.github.timwspence.cats.stm.STM.internal._

/**
  * Monad representing transactions involving one or more
  * `TVar`s.
  *
  * This design was inspired by [Beautiful Concurrency](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/beautiful.pdf) and informed by ZIO
  * which has a common origin in that paper via the [stm package](http://hackage.haskell.org/package/stm).
  */
sealed abstract class STM[+A] {

  /**
    * Functor map on `STM`.
    */
  final def map[B](f: A => B): STM[B] = Bind(this, f.andThen(Pure(_)))

  /**
    * Monadic bind on `STM`.
    */
  final def flatMap[B](f: A => STM[B]): STM[B] = Bind(this, f)

  /**
    * Try an alternative `STM` action if this one retries.
    */
  final def orElse[B >: A](fallback: STM[B]): STM[B] = OrElse(this, fallback)

  /**
    * Run transaction atomically
    */
  def atomically[F[+_]: Concurrent]: F[A] = STM.atomically[F](this)

}

object STM {

  /**
    * Commit the `STM` action as an `IO` action. The mutable
    * state of `TVar`s is only modified when this is invoked
    * (hence the `IO` context - modifying mutable state
    * is a side effect).
    */
  def atomically[F[_]] = new AtomicallyPartiallyApplied[F]

  /**
    * Convenience definition.
    */
  def retry[A]: STM[A] = Retry

  /**
    * Fallback to an alternative `STM` action if the first one
    * retries. The whole `orElse` action is retried if both
    * {@code attempt} and {@code fallback} retry.
    */
  def orElse[A](attempt: STM[A], fallback: STM[A]): STM[A] = attempt.orElse(fallback)

  /**
    * Retry transaction until {@code check} succeeds.
    */
  def check(check: => Boolean): STM[Unit] = if (check) unit else retry

  /**
    * Abort a transaction. Will raise {@code error} whenever
    * evaluated with [[atomically]].
    */
  def abort[A](error: Throwable): STM[A] = Abort(error)

  /**
    * Monadic return.
    */
  def pure[A](a: A): STM[A] = Pure(a)

  /**
    * Alias for `pure(())`.
    */
  val unit: STM[Unit] = pure(())

  //TODO can we make an ApplicativeError/MonadError here?
  implicit val stmMonad: Monad[STM] with MonoidK[STM] = new Monad[STM] with MonoidK[STM] {
    override def flatMap[A, B](fa: STM[A])(f: A => STM[B]): STM[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => STM[Either[A, B]]): STM[B] = ???

    override def pure[A](x: A): STM[A] = STM.pure(x)

    override def empty[A]: STM[A] = STM.retry

    override def combineK[A](x: STM[A], y: STM[A]): STM[A] = x.orElse(y)
  }

  implicit def stmMonoid[A](implicit M: Monoid[A]): Monoid[STM[A]] =
    new Monoid[STM[A]] {
      override def empty: STM[A] = STM.pure(M.empty)

      override def combine(x: STM[A], y: STM[A]): STM[A] = ???
    }

  final class AtomicallyPartiallyApplied[F[_]] {

    def apply[A](stm: STM[A])(implicit F: Concurrent[F]): F[A] = {
      val (r, log) = eval(stm)
      r match {
        case TSuccess(res) =>
          var commit = false
          STM.synchronized {
            if (!log.isDirty) {
              commit = true
              log.commit()
            }
          }
          if (commit)
            log.collectPending().traverse_(_.run.asInstanceOf[F[Unit]].start) >> F.pure(res)
          else apply(stm)
        case TFailure(e) => F.raiseError(e)
        case TRetry =>
          for {
            defer <- Deferred[F, Either[Throwable, A]]
            retryFiber = RetryFiber.make(stm, defer)
            txId       = IdGen.incrementAndGet()
            _ <- F.delay(log.registerRetry(txId, retryFiber))
            //TODO onCancel cancel/remove retry fiber
            e   <- defer.get
            res <- e.fold(F.raiseError(_), F.pure(_))
          } yield res
      }

    }

  }

  private[stm] object internal {

    val IdGen = new AtomicLong()

    final case class Pure[A](a: A)                                extends STM[A]
    final case class Bind[A, B](stm: STM[B], f: B => STM[A])      extends STM[A]
    final case class Get[A](tvar: TVar[A])                        extends STM[A]
    final case class Modify[A](tvar: TVar[A], f: A => A)          extends STM[Unit]
    final case class OrElse[A](attempt: STM[A], fallback: STM[A]) extends STM[A]
    final case class Abort(error: Throwable)                      extends STM[Nothing]
    final case object Retry                                       extends STM[Nothing]

    sealed trait TResult[+A]                    extends Product with Serializable
    final case class TSuccess[A](value: A)      extends TResult[A]
    final case class TFailure(error: Throwable) extends TResult[Nothing]
    case object TRetry                          extends TResult[Nothing]

    abstract class RetryFiber {

      type Effect[X]
      type Result

      val defer: Deferred[Effect, Either[Throwable, Result]]
      val stm: STM[Result]
      implicit def F: Concurrent[Effect]

      def run: Effect[Unit] = {
        val (r, log) = eval(stm)
        println(s"retrying: $r")
        r match {
          case TSuccess(res) =>
            var commit = false
            STM.synchronized {
              if (!log.isDirty) {
                commit = true
                log.commit()
              }
            }
            if (commit)
              log.collectPending().traverse_(_.run.asInstanceOf[Effect[Unit]].start) >> defer.complete(Right(res))
            else run
          case TFailure(e) => defer.complete(Left(e))
          case TRetry      => F.delay(log.registerRetry(IdGen.incrementAndGet(), this))
        }
      }
    }

    object RetryFiber {
      type Aux[F[_], A] = RetryFiber { type Effect[X] = F[X]; type Result = A }

      def make[F[_], A](stm0: STM[A], defer0: Deferred[F, Either[Throwable, A]])(implicit
        F0: Concurrent[F]
      ): RetryFiber.Aux[F, A] =
        new RetryFiber {
          type Effect[X] = F[X]
          type Result    = A

          val defer: Deferred[F, Either[Throwable, A]] = defer0
          val stm: STM[A]                              = stm0
          implicit def F: Concurrent[F]                = F0
        }
    }

    def eval[A](stm: STM[A]): (TResult[A], TLog) = {
      var conts: List[Cont]                             = Nil
      var fallbacks: List[(STM[Any], TLog, List[Cont])] = Nil
      var log: TLog                                     = TLog.empty

      @tailrec
      def go(stm: STM[Any]): TResult[Any] =
        stm match {
          case Pure(a) =>
            if (conts.isEmpty) TSuccess(a)
            else {
              val f = conts.head
              conts = conts.tail
              go(f(a))
            }
          case Bind(stm, f) =>
            conts = f :: conts
            go(stm)
          case Get(tvar)                 => go(Pure(log.get(tvar)))
          case Modify(tvar, f)           => go(Pure(log.modify(tvar.asInstanceOf[TVar[Any]], f)))
          case OrElse(attempt, fallback) =>
            //TODO this isn't quite right as we want to capture tvars that were
            //modified in the attempt branch (with their values reverted)
            //so that we register for retry with all possible tvars
            fallbacks = (fallback, log.snapshot(), conts) :: fallbacks
            go(attempt)
          case Abort(error) =>
            TFailure(error)
          case Retry =>
            if (fallbacks.isEmpty) TRetry
            else {
              val (fb, lg, cts) = fallbacks.head
              log = lg
              conts = cts
              fallbacks = fallbacks.tail
              go(fb)
            }
        }

      go(stm).asInstanceOf[TResult[A]] -> log
    }

    case class TLog(private var map: Map[TVar[Any], TLogEntry]) {

      def values: Iterable[TLogEntry] = map.values

      def get(tvar: TVar[Any]): Any =
        if (map.contains(tvar))
          map(tvar).unsafeGet[Any]
        else {
          val current = tvar.value
          map = map + (tvar -> TLogEntry(tvar, current))
          current
        }

      def modify(tvar: TVar[Any], f: Any => Any): Unit = {
        val current = get(tvar)
        val entry   = map(tvar)
        entry.unsafeSet(f(current))
        ()
      }

      def isDirty: Boolean = values.exists(_.isDirty)

      def snapshot(): TLog = TLog(map)
// def snapshot: () => Unit = {
// -        val snapshot: Map[TxId, Any] = map.toMap.map {
// -          case (id, e) => id -> e.current
// +      def run: Effect[Unit] = {
// +        val (r, log) = eval(stm)
// +        println(s"retrying: $r")
// +        r match {
// +          case TSuccess(res) =>
// +            var commit = false
// +            STM.synchronized {
// +              if (!log.isDirty) {
// +                commit = true
// +                log.commit()
// +              }
// +            }
// +            if (commit)
// +              log.collectPending().traverse_(_.run.asInstanceOf[Effect[Unit]].start) >> defer.complete(Right(res))
// +            else run
// +          case TFailure(e) => defer.complete(Left(e))
// +          case TRetry      => F.unit
//          }
// -        () =>
// -          for (pair <- map)
// -            if (snapshot contains (pair._1))
// -              //The entry was already modified at
// -              //some point in the transaction
// -              pair._2.unsafeSet(snapshot(pair._1))
// -            else
// -              //The entry was introduced in the attempted
// -              //part of the transaction that we are now
// -              //reverting so we reset to the initial
// -              //value.
// -              //We don't want to remove it from the map
// -              //as we still want to add the currently
// -              //executing transaction to the set of
// -              //pending transactions for this tvar if
// -              //the whole transaction fails.
// -              pair._2.reset()
//        }

      def commit(): Unit = values.foreach(_.commit())

      def registerRetry(txId: TxId, fiber: RetryFiber): Unit =
        values.foreach { e => {
          println(s"Registering txn $txId with tvar ${e.tvar.id}")
          e.tvar.pending
            .updateAndGet(asJavaUnaryOperator(m => m + (txId -> fiber)))
                        }
        }

      def collectPending(): List[RetryFiber] = {
        var pending: Map[TxId, RetryFiber] = Map.empty
        values.foreach { e =>
          val p = e.tvar.pending.getAndSet(Map.empty)
          println(s"Pending for tvar ${e.tvar.id}: $p")
          pending = pending ++ p
          println(s"Updated pending: $pending")

        }
        println(s"Retrying ${pending.keys.toList}")
        pending.values.toList
      }

    }

    object TLog {
      def empty: TLog = TLog(Map.empty)
    }

    type Cont = Any => STM[Any]

    type TxId = Long

    //TVar is invariant (mutable) so we can't just deal with TVar[Any]
    abstract class TLogEntry {
      type Repr
      var current: Repr
      val initial: Repr
      val tvar: TVar[Repr]

      def unsafeGet[A]: A = current.asInstanceOf[A]

      def unsafeSet[A](a: A): Unit = current = a.asInstanceOf[Repr]

      def commit(): Unit = tvar.value = current

      def reset(): Unit = current = initial

      def isDirty: Boolean = initial != tvar.value.asInstanceOf[Repr]

    }

    object TLogEntry {

      def apply[A](tvar0: TVar[A], current0: A): TLogEntry =
        new TLogEntry {
          override type Repr = A
          override var current: A    = current0
          override val initial: A    = tvar0.value.asInstanceOf[A]
          override val tvar: TVar[A] = tvar0
        }

    }

  }

}
