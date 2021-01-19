package com.spotify.cats.effect.queue

trait MultiConsumerBoundedQueue[F[_], A] {
  
  def put(element: A): Unit
  def consume(consumer: A => F[Unit]): F[Long]
  def drainAndClose(): Unit
  def pendingItemsCount: Int

}
