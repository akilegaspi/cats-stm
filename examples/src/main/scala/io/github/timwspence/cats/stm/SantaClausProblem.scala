package io.github.timwspence.cats.stm
// import io.github.timwspence.cats.stm._
import cats.effect._
import cats.implicits._
import scala.concurrent.duration._

object SantaClausProblem extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    (mainProblem.timeout(5.seconds).attempt >> debug()).as(ExitCode.Success)

  def meetInStudy(id: Int): IO[Unit] = IO(println(show"Elf $id meeting in the study"))

  sealed abstract case class Gate(capacity: Int, tv: TVar[Int]) {
    def pass: IO[Unit]    = Gate.pass(this)
    def operate: IO[Unit] = Gate.operate(this)
  }
  object Gate {
    def of(capacity: Int) =
      TVar.of(1).map(new Gate(capacity, _) {})

    def pass(g: Gate): IO[Unit] =
      STM.atomically[IO] {
        for {
          nLeft <- g.tv.get
          _     <- STM.check({println(s"nleft is $nLeft"); nLeft > 0})
          _     <- g.tv.modify(_ - 1)
        } yield ()
      }

    def operate(g: Gate): IO[Unit] =
      for {
        _ <- IO(println("Operating gate"))
        _ <- STM.atomically[IO](for {
                                  c <- g.tv.get
                                  _ <- {println(s"pending is: ${d.get.pending}"); STM.check(c == 0)}
                                } yield ())
        _ <- IO(println("capacity is zero"))
        _ <- STM.atomically[IO](g.tv.set(g.capacity))
        _ <- debug()
        _ <- IO(println("capacity reset"))
        _ <- STM.atomically[IO] {
          for {
            nLeft <- {println("hi"); g.tv.get}
            _     <- STM.check({println(s"nLeft in operate: $nLeft"); nLeft === 0})
          } yield ()
        }
        _ <- IO(println("Gate operated"))
      } yield ()
  }

  def elf(g: Gate, i: Int): IO[Fiber[IO, Nothing]] =
    (
      for {
        _ <- IO(println("trying to pass gate"))
        _ <- g.pass
        _ <- debug()
        _ <- meetInStudy(i)
      } yield ()
    ).foreverM.start

  def santa(elfGroup: Gate): IO[Unit] = {
    for {
      _ <- IO(println(show"Ho! Ho! Ho! let’s do elf stuff"))
      _ <- elfGroup.operate
    } yield ()
  }

  def mainProblem: IO[Unit] =
    for {
      g         <- Gate.of(1).atomically[IO]
      _         <- IO(this.d = Some(g.tv))
      _         <- List(1).traverse_(n => elf(g, n))
      _         <- santa(g).foreverM.void
      _         <- debug()
    } yield ()

  var d: Option[TVar[Int]] = None

  def debug(): IO[Unit] = IO(println(s"debug: ${d.get.pending}"))

}
