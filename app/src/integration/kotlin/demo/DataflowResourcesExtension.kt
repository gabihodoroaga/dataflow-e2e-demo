package dataflow.e2e.demo

import java.text.SimpleDateFormat
import java.util.Date
import kotlin.reflect.full.memberFunctions
import org.apache.beam.runners.dataflow.DataflowClient
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.junit.Assert.*
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Namespace
import org.slf4j.LoggerFactory

/**
 * Helper class that checks if the dataset exists, create a unique table name, and deletes the table
 * after all test run. The table is not created by this class
 */
class DataflowResourcesExtension : BeforeAllCallback, AfterAllCallback {

  companion object {
    @JvmStatic val logger = LoggerFactory.getLogger(PubsubResourceExtension::class.java)
    val STORE = Namespace.create("com.example.dataflow")
    val DATAFLOW_JOB = "dataFlowJob"

    fun deleteDataFlowJob(projectId: String, region: String, jobName: String?) {
      if (jobName == null) {
        return
      }
      try {
        val dpOptions =
            PipelineOptionsFactory.fromArgs(
                    *arrayOf(
                        "--jobName=$jobName",
                        "--runner=DataflowRunner",
                        "--region=$region",
                        "--project=$projectId"
                    )
                )
                .`as`(DataflowPipelineOptions::class.java)

        val dataflowClient = DataflowClient.create(dpOptions)
        val jobsResponse = dataflowClient.listJobs(null)
        val job = jobsResponse.getJobs().firstOrNull { it.name == jobName }

        if (job != null) {
          job.setRequestedState("JOB_STATE_CANCELLED")
          dataflowClient.updateJob(job.id, job)
          logger.info("Dataflow job with '{}' canceled.", jobName)
        } else {
          logger.info("Dataflow job '{}' not found", jobName)
        }
      } catch (e: Exception) {
        logger.info("Error canceling Dataflow job {}.", e)
      }
    }
  }

  override fun beforeAll(context: ExtensionContext) {
    logger.info("Setup required Dataflow resources...")

    val testInstance = context.getTestInstance().get()
    val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")
    val region = Utils.readInstanceProperty<String>(testInstance, "region")
    val className = Utils.toDashCase(testInstance::class.simpleName!!)

    // run the job
    val jobName = "$className-${SimpleDateFormat("yyMMddHHmmss").format(Date())}"

    val runDataFlowMethod = testInstance::class.memberFunctions.find { it.name == "runDataflowJob" }
    if (runDataFlowMethod == null) {
      logger.info(
          "function 'runDataflowJob(jobName: String)') not found. Skipping job run. You must define and use a function named 'runDataflowJob(jobName: String)') for streaming job setup and cleaning up. Ignore for batch jobs."
      )
      return
    }
    // save the job here even if is possible that the job to have an error on create
    context.getStore(STORE).put(DATAFLOW_JOB, jobName)
    logger.info("Setup Dataflow job with name {} at {}/{}", jobName, projectId, region)

    // run the job
    runDataFlowMethod.call(testInstance, jobName)
  }

  override fun afterAll(context: ExtensionContext) {
    logger.info("Cleaning up the Dataflow resources...")
    val testInstance = context.getTestInstance().get()
    val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")
    val region = Utils.readInstanceProperty<String>(testInstance, "region")

    deleteDataFlowJob(projectId, region, context.getStore(STORE).get(DATAFLOW_JOB) as String?)
  }
}
