package org.zella.topubsub.functions


import java.util.logging.Logger

import cats.effect.IO
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest, ReceivedMessage}
import io.circe.parser._
import org.threeten.bp.Duration
import org.zella.topubsub.functions.ResponseReceiver.ResponseNotFound

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ResponseReceiver(projectId: String,
                       subscriptionId: String,
                       maxMessageBytes: Int,
                       receiveTimeoutSeconds: Int
                      ) {

  private val logger = Logger.getLogger(this.getClass.getName)

  private implicit val cs = IO.contextShift(ExecutionContext.global)
  private implicit val timer = IO.timer(ExecutionContext.global)

  private val subscriberStubSettingsBuilder = SubscriberStubSettings.newBuilder()
  subscriberStubSettingsBuilder
    .pullSettings()
    .setRetrySettings(
      subscriberStubSettingsBuilder.pullSettings().getRetrySettings().toBuilder()
        .setTotalTimeout(Duration.ofSeconds(receiveTimeoutSeconds))
        .build());
  private val subscriberStubSettings = subscriberStubSettingsBuilder.setTransportChannelProvider(
    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
      .setMaxInboundMessageSize(maxMessageBytes)
      .build()).build();

  private val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

  private val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);

  private def pullMessages: IO[Seq[ReceivedMessage]] = IO {
    val pullRequest = PullRequest.newBuilder.setMaxMessages(1).setSubscription(subscriptionName).build
    val pullResponse = subscriber.pullCallable.call(pullRequest)
    logger.info(s"Pulled ${pullResponse.getReceivedMessagesCount} messages")
    pullResponse.getReceivedMessagesList.asScala.toSeq
  }.handleErrorWith {
    e =>
      logger.severe("Fail to pull messages")
      e.printStackTrace()
      IO.pure(Seq.empty)
  }

  private def messageToRequest(m: ReceivedMessage): ResponseMessage = IO.fromEither(
    for {
      json <- parse(m.getMessage.getData.toStringUtf8)
      _ <- Right(logger.fine(json.toString()))
      m <- json.as[ResponseMessage]
    } yield m).unsafeRunSync


  def receiveExactly(uuid: String): IO[(ResponseMessage, String)] = retry(
    for {
      find <- pullMessages.map(msgs => msgs.map(m => (messageToRequest(m), m.getAckId)).find(_._1.uuid == uuid))
      resp <- IO.fromOption(find)(new ResponseNotFound(uuid))
      _ <- IO(logger.info(s"Found message $uuid"))
    } yield resp, 16)
    .timeout(receiveTimeoutSeconds.second)

  def sendAck(ackId: String): IO[Unit] = IO {
    val acknowledgeRequest = AcknowledgeRequest.newBuilder.setSubscription(subscriptionName).addAckIds(ackId).build
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
  }

}

object ResponseReceiver {

  class ResponseNotFound(uuid: String) extends RuntimeException(s"Response with uuid $uuid not found")

}
