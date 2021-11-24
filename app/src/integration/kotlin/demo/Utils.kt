package dataflow.e2e.demo

import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import java.util.concurrent.TimeUnit
import kotlin.reflect.KProperty1
import kotlinx.coroutines.*
import org.junit.Assert.*
import org.slf4j.LoggerFactory

class Utils {
  companion object {
    @JvmStatic val logger = LoggerFactory.getLogger(Utils::class.java)

    @Suppress("UNCHECKED_CAST")
    fun <R> readInstanceProperty(instance: Any, propertyName: String): R {
      val property = instance::class.members.find { it.name == propertyName }
      if (property == null || (property as KProperty1<Any, *>).get(instance) == null) {
        fail(
            "You must define and initialize a property named '$propertyName'. Example val $propertyName = System.getenv(\"${propertyName.uppercase()}\")"
        )
      }
      return (property as KProperty1<Any, *>).get(instance) as R
    }

    fun toDashCase(value: String): String {
      var text: String = ""
      var isFirst = true
      value.forEach {
        if (it.isUpperCase()) {
          if (isFirst) isFirst = false else text += "-"
          text += it.lowercase()
        } else {
          text += it
        }
      }
      return text
    }

    fun <T> waitFor(
        timeout: Long, // seconds
        interval: Long, // seconds
        condition: () -> Pair<Boolean, T?>
    ): Pair<Boolean, T?> {

      // TODO: improve this use kotlin coroutine
      // very basic implementation of a wait loop
      var retryCount = 0
      var result: T? = null
      while (retryCount < (timeout / interval)) {
        val (ok, tmp) = condition()
        if (ok) {
          return Pair(true, tmp)
        }
        result = tmp
        logger.info("waitFor condition returned false, retry ($retryCount) in $interval seconds...")
        runBlocking { delay(interval*1000) }
        retryCount++
      }
      return Pair(false, result)
    }

    fun getBigQueryCountAll(projectId: String, dataset: String, table: String?): Long? {
      try {
        val bigquery = BigQueryOptions.getDefaultInstance().getService()
        val query = "SELECT COUNT(*) AS total FROM `$projectId.$dataset.$table`;"
        val config = QueryJobConfiguration.newBuilder(query).build()

        val results = bigquery.query(config)
        var result = 0L
        results.iterateAll().forEach { row -> row.forEach { result = it.getLongValue() } }

        return result
      } catch (bqe: BigQueryException) {
        if (bqe.toString().contains("Not found:")) {
          logger.info(
              "BigQuery table {}.{}.{} not found...`",
              projectId,
              dataset,
              table,
          )
        } else {
          logger.info(
              "Error getting the number of records from BigQuery table {}.{}.{}",
              projectId,
              dataset,
              table,
              bqe
          )
        }
        return null
      } catch (e: Exception) {
        logger.info(
            "Error getting the number of records from BigQuery table {}.{}.{}",
            projectId,
            dataset,
            table,
            e
        )
        return null
      }
    }

    fun publishMessages(projectId: String, topicId: String?, messages: List<String>) {
      val topicName = TopicName.of(projectId, topicId)
      var publisher: Publisher? = null
      try {
        publisher = Publisher.newBuilder(topicName).build()
        for (message in messages) {
          val data = ByteString.copyFromUtf8(message)
          val pubsubMessage = PubsubMessage.newBuilder().setData(data).build()
          val messageIdFuture = publisher.publish(pubsubMessage)
          val messageId = messageIdFuture.get()
          logger.info("Published message with ID: {}", messageId)
        }
      } finally {
        if (publisher != null) {
          publisher.shutdown()
          publisher.awaitTermination(1, TimeUnit.MINUTES)
        }
      }
    }
  }
}
