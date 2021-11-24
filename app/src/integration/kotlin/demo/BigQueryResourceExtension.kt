package dataflow.e2e.demo

import com.google.api.services.bigquery.model.*
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.Date
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.functions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
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
class BigQueryResourceExtension : BeforeAllCallback, AfterAllCallback {

  companion object {
    @JvmStatic val logger = LoggerFactory.getLogger(BigQueryResourceExtension::class.java)
    val STORE = Namespace.create("com.example.bq")
    val BIGQUERY_TABLES = "bigQueryTables"
    val searchRegex = Regex("bigQuery([a-zA-Z0-9]*)Table")

    fun createBigQueryTable(
        projectId: String,
        datasetName: String,
        baseName: String,
        tableSchema: String?
    ): String? {

      try {
        var bigquery = BigQueryOptions.getDefaultInstance().getService()
        val dataset = bigquery.getDataset(DatasetId.of(datasetName))

        if (dataset == null) {
          logger.info("Dataset {} not found for project {}", datasetName, projectId)
          return null
        }

        val tableName = "${baseName}-${SimpleDateFormat("yyMMddHHmmss").format(Date())}"
        if (tableSchema != null) {
          val tableId = TableId.of(datasetName, tableName)

          val tableDefinition = StandardTableDefinition.of(getTableSchema(tableSchema))
          val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build()

          bigquery.create(tableInfo)
        }

        // Only the random name is created here - the table will be created by the job
        return tableName
      } catch (e: Exception) {
        logger.info(
            "Error creating bq table for project {}, datasetName {},{}.",
            projectId,
            datasetName,
            baseName,
            e
        )
        return null
      }
    }

    fun deleteBigQueryTable(projectId: String, datasetName: String, tableName: String?) {
      if (tableName == null) {
        return
      }
      try {
        val bigquery = BigQueryOptions.getDefaultInstance().getService()
        val success = bigquery.delete(TableId.of(datasetName, tableName))
        if (!success) {
          logger.info("BigQuery table '{}:{}.{}' not found.", projectId, datasetName, tableName)
        } else {
          logger.info("Deleted BigQuery table '{}:{}.{}'.", projectId, datasetName, tableName)
        }
      } catch (e: Exception) {
        logger.info(
            "An error ocurred when trying to delete the BigQuery table '{}:{}.{}'. The resource must be deleted manually.",
            projectId,
            datasetName,
            tableName,
            e
        )
      }
    }

    // custom json deserializer for BigQuery table schema
    fun getGson(): Gson {
      return GsonBuilder()
          .registerTypeAdapter(
              TableFieldSchema::class.java,
              JsonDeserializer<TableFieldSchema>({
                  jsonElement: JsonElement,
                  _: Type,
                  context: JsonDeserializationContext ->
                val item = jsonElement.getAsJsonObject()
                val fieldSchema = Gson().fromJson(item, TableFieldSchema::class.java)
                if (fieldSchema.fields != null) {
                  fieldSchema.fields =
                      context
                          .deserialize<Array<TableFieldSchema>>(
                              item.get("fields").getAsJsonArray(),
                              Array<TableFieldSchema>::class.java
                          )
                          .toList()
                }
                fieldSchema
              })
          )
          .create()
    }

    fun getTableSchema(jsonData: String): Schema {

      val fieldsList = getGson().fromJson(jsonData, Array<TableFieldSchema>::class.java).toList()
      val tableSchema = TableSchema().setFields(fieldsList)

      return Schema::class
          .functions
          .firstOrNull { it.name == "fromPb" }
          ?.apply { isAccessible = true }
          ?.call(tableSchema) as
          Schema
    }
  }

  override fun beforeAll(context: ExtensionContext) {
    logger.info("Setup required BigQuery resources...")

    // Get the test instance
    val testInstance = context.getTestInstance().get()
    val className = Utils.toDashCase(testInstance::class.simpleName!!)
    val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")

    // find all the fields that starts with bigQuery[name]Table
    val bigQueryTableFields =
        testInstance::class.memberProperties.filter { it.name.matches(searchRegex) }
    val bigQueryTables = mutableListOf<String>()
    bigQueryTableFields.forEach {
      if (it is KMutableProperty<*>) {
        val bigQueryDataset = Utils.readInstanceProperty<String>(testInstance, "bigQueryDataset")

        // compute the table name
        var tableName = className
        val groupName = searchRegex.find(it.name)!!.groupValues[1]
        if (groupName != "") {
          tableName += "-" + groupName.lowercase()
        }
        // get table schema if exits
        val tableSchema =
            {
              val res = {}.javaClass.getResource("/${it.name}Schema.json")
              if (res != null) {
                res.readText()
              } else {
                null
              }
            }()

        val bigQueryTable = createBigQueryTable(projectId, bigQueryDataset, tableName, tableSchema)
        if (bigQueryTable == null) {
          fail("Cannot create setup BigQuery table for project $projectId, tableName $tableName")
        }
        bigQueryTables.add(bigQueryTable!!)
        it.setter.call(testInstance, bigQueryTable)
      }
    }
    context.getStore(STORE).put(BIGQUERY_TABLES, bigQueryTables)
  }

  @Suppress("UNCHECKED_CAST")
  override fun afterAll(context: ExtensionContext) {
    logger.info("Cleaning up the BigQuery resources...")

    val noCleanOptionBq = System.getenv("NO_CLEANUP_BQ")
    if (noCleanOptionBq == null || noCleanOptionBq != "1") {

      val testInstance = context.getTestInstance().get()
      val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")
      val bigQueryDataset = Utils.readInstanceProperty<String>(testInstance, "bigQueryDataset")

      val bigQueryTables = context.getStore(STORE).get(BIGQUERY_TABLES) as List<String>
      bigQueryTables.forEach { deleteBigQueryTable(projectId, bigQueryDataset, it) }
    } else {
      logger.info("!!!NO_CLEANUP_BQ requested. The resources must be deleted manually")
    }
  }
}
