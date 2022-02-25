package dataflow.e2e.demo

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory

/**
 * e2e tests for the pipeline
 *
 * This test runs in a real environment and uses the following GCP resources:
 * - Pub/Sub topic
 * - Pub/Sub subscription
 * - DataFlow job
 * - BigQuery table
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(
        PubsubResourceExtension::class,
        BigQueryResourceExtension::class,
        DataflowResourcesExtension::class
)
class E2eTestsDemo {

    val projectId = System.getenv("PROJECT_ID")
    val region = System.getenv("REGION")
    val bigQueryDataset = System.getenv("BIGQUERY_DATASET")
    val bucketName = System.getenv("BUCKET_NAME")

    var pubsubTopic: String? = null
    var pubsubSubscription: String? = null
    val pubsubOtherTopic: String? = null
    val pubsubOtherSubscription: String? = null
    var bigQueryOutputTable: String? = null
    var bigQueryOutput2Table: String? = null

    @Test
    fun testPipeline() {

        // the dataflow job is deplyed automaticaly by the 'DataflowResourcesExtension::class'

        // publish the messages
        val messages = getValidMessages()
        val expectedRecordCount =
                messages.map { it.noOfRecords }.reduce { sum, it -> sum + it }.toLong()
        val expectedRecordCount2 =
                messages.map { it.noOfRecords2 }.reduce { sum, it -> sum + it }.toLong()

        Utils.publishMessages(projectId, pubsubTopic, messages.map { it.body })
        logger.info("Published ${messages.size} messages to Pub//Sub topic $pubsubTopic...")

        // get the big query results
        // wait 15 seconds to process the messages
        runBlocking { delay(15_000) }

        // get the bigquery result
        var (_, actualRecordCount) =
                Utils.waitFor<Long>(
                        300,
                        30,
                        cond@{
                            val recordCount =
                                    Utils.getBigQueryCountAll(
                                            projectId,
                                            bigQueryDataset,
                                            bigQueryOutputTable
                                    )
                            if (recordCount != null && recordCount == expectedRecordCount) {
                                return@cond Pair(true, recordCount)
                            }
                            return@cond Pair(false, recordCount)
                        }
                )

        // get the bigquery result
        var (_, actualRecordCount2) =
                Utils.waitFor<Long>(
                        300,
                        30,
                        cond@{
                            val recordCount =
                                    Utils.getBigQueryCountAll(
                                            projectId,
                                            bigQueryDataset,
                                            bigQueryOutput2Table
                                    )
                            if (recordCount != null && recordCount == expectedRecordCount) {
                                return@cond Pair(true, recordCount)
                            }
                            return@cond Pair(false, recordCount)
                        }
                )

        // Assert total number of records
        assertEquals(
                "Expected number of records does not match.",
                expectedRecordCount,
                actualRecordCount
        )
        assertEquals(
                "Expected number of records does not match.",
                expectedRecordCount2,
                actualRecordCount2
        )
    }

    fun runDataflowJob(jobName: String) {
        try {
            main(
                    *arrayOf(
                            "--jobName=$jobName",
                            "--runner=DataflowRunner",
                            "--project=$projectId",
                            "--region=$region",
                            "--workerMachineType=e2-medium",
                            "--maxNumWorkers=1",
                            "--enableStreamingEngine",
                            "--stagingLocation=gs://$bucketName/staging",
                            "--tempLocation=gs://$bucketName/temp",
                            "--inputSubscription=projects/$projectId/subscriptions/$pubsubSubscription",
                            "--outputTable=$bigQueryDataset.$bigQueryOutputTable",
                            "--output2Table=$bigQueryDataset.$bigQueryOutput2Table",
                    )
            )
        } catch (e: Exception) {
            throw Exception("Cannot create dataflow job for project ${projectId}", e)
        }
        // TODO: implement a proper waiting for the job state using the DataflowClient
        // wait for the running state JOB_STATE_RUNNING + wait for job messages like "worker ready"
        runBlocking { delay(120_000) }
    }

    class TestMessage(
            val desc: String,
            val body: String,
            val noOfRecords: Int,
            val noOfRecords2: Int
    ) {}

    companion object {
        @JvmStatic val logger = LoggerFactory.getLogger(E2eTestsDemo::class.java)

        fun getValidMessages(): List<TestMessage> {
            return listOf(
                    TestMessage(
                            "valid message - type 1",
                            """{"type": "1","data":"data for type 1"}""",
                            1,
                            0
                    ),
                    TestMessage(
                            "valid message - type 2",
                            """{"type": "2","data":"data for type 2"}""",
                            0,
                            1
                    ),
            )
        }
    }
}
