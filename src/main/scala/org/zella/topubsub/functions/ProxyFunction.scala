package org.zella.topubsub.functions

import java.util.logging.Logger

import com.google.cloud.functions.{HttpFunction, HttpRequest, HttpResponse}
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import io.circe.syntax._
import org.apache.commons.codec.binary.Base64

import scala.jdk.CollectionConverters._

class ProxyFunction extends HttpFunction {

  private val log = Logger.getLogger(this.getClass.getName)

  private val projectId = sys.env("PROJECT_ID")
  private val topicId = sys.env("TOPIC_ID")

  private val topicName: TopicName = TopicName.of(projectId, topicId)
  private val publisher: Publisher = Publisher.newBuilder(topicName).build()

  private val receiver = new ResponseReceiver(
    projectId = projectId,
    subscriptionId = sys.env("SUBSCRIPTION_ID"),
    maxMessageBytes = sys.env.getOrElse("MAX_MESSAGE_BYTES", (1024 * 1024 * 4).toString).toInt,
    sys.env.getOrElse("RECEIVE_TIMEOUT_SECONDS", 10.toString).toInt
  )

  override def service(in: HttpRequest, out: HttpResponse): Unit = {

    val req = RequestMessage.create(
      method = in.getMethod,
      uri = in.getUri,
      headers = in.getHeaders.asScala.view.mapValues(_.asScala.toSeq).toMap,
      body = in.getInputStream
    )

    publishSync(req)

    val (resp, ackId) = receiver.receiveExactly(req.uuid).unsafeRunSync()

    out.setStatusCode(resp.status)
    resp.headers.foreach { case (k, v) => out.appendHeader(k, v.mkString(",")) }
    val bytes: Array[Byte] = Base64.decodeBase64(resp.base64Body)
    out.getOutputStream.write(bytes)

    receiver.sendAck(ackId).unsafeRunSync()

    out.getOutputStream.flush()
    out.getOutputStream.close()
  }


  private def publishSync(message: RequestMessage): Unit = {

    val messageJson = message.asJson.noSpaces
    log.fine(messageJson)

    val pubsubMessage = PubsubMessage.newBuilder()
      .setData(ByteString.copyFromUtf8(messageJson))
      .build()

    val messageId = publisher.publish(pubsubMessage).get()
    log.info(s"Message $messageId sended")
  }

}
