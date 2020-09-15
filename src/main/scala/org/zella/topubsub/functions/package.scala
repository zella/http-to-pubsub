package org.zella.topubsub

import java.io.InputStream
import java.util.UUID

import cats.effect.IO
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils

package object functions {
  def retry[A](ioa: IO[A], maxRetries: Int): IO[A] = {
    ioa.handleErrorWith { error =>
      if (maxRetries > 0)
        retry(ioa, maxRetries - 1)
      else
        IO.raiseError(error)
    }
  }

  case class RequestMessage(uuid: String,
                            method: String,
                            uri: String,
                            headers: Map[String, Seq[String]],
                            base64Body: String)

  object RequestMessage {

    implicit val jsonEncoder: Encoder[RequestMessage] = deriveEncoder

    def create(method: String, uri: String, headers: Map[String, Seq[String]], body: InputStream): RequestMessage = {
      val uuid = UUID.randomUUID().toString

      val bytes = IOUtils.toByteArray(body)
      val bytes64 = Base64.encodeBase64(bytes)
      val body64 = new String(bytes64)

      val bodyLength = bytes.length

      val headersNew = headers.filterNot {
        case (k, _) => k.equalsIgnoreCase("host")
      } + ("content-length" -> Seq(bodyLength.toString))

      new RequestMessage(uuid, method, uri, headersNew, body64)
    }
  }

  case class ResponseMessage(uuid: String,
                             status: Int,
                             headers: Map[String, Seq[String]],
                             base64Body: String)

  object ResponseMessage {
    implicit val jsonEncoder: Decoder[ResponseMessage] = deriveDecoder
  }

}
