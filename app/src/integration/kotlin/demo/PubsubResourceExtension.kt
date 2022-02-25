package dataflow.e2e.demo

import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.TopicName
import java.text.SimpleDateFormat
import java.util.Date
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.memberProperties
import org.junit.Assert.*
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Namespace
import org.slf4j.LoggerFactory

/**
 * Helper class that create a topic and/or a subscription and injects the name into 'pubsubTopic'
 * and 'pubsubSubscription' fields if they are present. The class delete the created topic and
 * subscription after all the tests run.
 */
class PubsubResourceExtension : BeforeAllCallback, AfterAllCallback {

    companion object {
        @JvmStatic val logger = LoggerFactory.getLogger(PubsubResourceExtension::class.java)
        val STORE = Namespace.create("com.example.pubsub")
        val PUBSUB_TOPIC = "pubsubTopic"
        val PUBSUB_SUBSCRIPTION = "pubsubSubscription"
        val searchRegexTopic = Regex("pubsub([a-zA-Z0-9]*)Topic")

        fun createPubsubTopic(projectId: String, baseName: String): String? {
            var topicAdminClient: TopicAdminClient? = null
            try {
                topicAdminClient = TopicAdminClient.create()
                val topicId = "$baseName-${SimpleDateFormat("yyMMddHHmmss").format(Date())}"
                val topicName = TopicName.of(projectId, topicId)
                val topic = topicAdminClient.createTopic(topicName)
                logger.info("Created topic {}", topic.getName())
                return topicId
            } catch (e: Exception) {
                logger.info("Error creating topic for project {}.", projectId, e)
                return null
            } finally {
                if (topicAdminClient != null) {
                    topicAdminClient.close()
                }
            }
        }

        fun deletePubsubTopic(projectId: String, topicId: String?) {
            if (topicId == null) {
                return
            }
            var topicAdminClient: TopicAdminClient? = null
            try {
                topicAdminClient = TopicAdminClient.create()
                val topicName = TopicName.of(projectId, topicId)
                topicAdminClient.deleteTopic(topicName)
                logger.info("Deleted topic {}/{}", projectId, topicId)
            } catch (e: Exception) {
                logger.info(
                        "An error ocurred when trying to delete the topic {}/{}. The topic must be deleted manually.",
                        projectId,
                        topicId,
                        e
                )
            } finally {
                if (topicAdminClient != null) {
                    topicAdminClient.close()
                }
            }
        }

        fun createPubsubSubscription(projectId: String, topicId: String): String? {
            var subscriptionAdminClient: SubscriptionAdminClient? = null
            try {
                subscriptionAdminClient = SubscriptionAdminClient.create()
                val subscriptionId = topicId + "-sub"
                val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
                val topicName = TopicName.of(projectId, topicId)
                val subscription =
                        subscriptionAdminClient.createSubscription(
                                subscriptionName,
                                topicName,
                                PushConfig.getDefaultInstance(),
                                10
                        )
                logger.info("Created pull subscription: " + subscription.getName())
                return subscriptionId
            } catch (e: Exception) {
                logger.info(
                        "Error creating subscription for project {}, topic {}.",
                        projectId,
                        topicId,
                        e
                )
                return null
            } finally {
                if (subscriptionAdminClient != null) {
                    subscriptionAdminClient.close()
                }
            }
        }

        fun deletePubsubSubscription(projectId: String, subscriptionId: String?) {
            if (subscriptionId == null) {
                return
            }
            var subscriptionAdminClient: SubscriptionAdminClient? = null
            try {
                subscriptionAdminClient = SubscriptionAdminClient.create()
                val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
                subscriptionAdminClient.deleteSubscription(subscriptionName)
                logger.info("Deleted subscription {}/{}", projectId, subscriptionId)
            } catch (e: Exception) {
                logger.info(
                        "An error ocurred when trying to delete the subscription {}/{}. The subscription must be deleted manually.",
                        projectId,
                        subscriptionId,
                        e
                )
            } finally {
                if (subscriptionAdminClient != null) {
                    subscriptionAdminClient.close()
                }
            }
        }
    }

    override fun beforeAll(context: ExtensionContext) {
        logger.info("Setup required Pub/Sub resources...")
        // Get the test instance
        val testInstance = context.getTestInstance().get()
        val className = Utils.toDashCase(testInstance::class.simpleName!!)
        val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")

        val pubsubTopicFields =
                testInstance::class.memberProperties.filter { it.name.matches(searchRegexTopic) }

        val pubsubTopics = mutableListOf<String?>()
        context.getStore(STORE).put(PUBSUB_TOPIC, pubsubTopics)

        val pubsubSubscriptions = mutableListOf<String?>()
        context.getStore(STORE).put(PUBSUB_SUBSCRIPTION, pubsubSubscriptions)

        pubsubTopicFields.forEach {
            if (it is KMutableProperty<*>) {
                // find the pubsub topic name
                var topicName = className
                val groupName = searchRegexTopic.find(it.name)!!.groupValues[1]
                if (groupName != "") {
                    topicName += "-" + groupName.lowercase()
                }

                val pubsubTopic = createPubsubTopic(projectId, topicName)
                if (pubsubTopic == null) {
                    fail("Cannot create Pub/Sub topic for project ${projectId}")
                }
                pubsubTopics.add(pubsubTopic)
                it.setter.call(testInstance, pubsubTopic)

                // Setup a Pub/Sub subscription
                val pubsubSubscriptionField =
                        testInstance::class.memberProperties.find {
                            it.name == "pubsub${groupName}Subscription"
                        }
                if (pubsubSubscriptionField != null &&
                                pubsubSubscriptionField is KMutableProperty<*>
                ) {
                    val pubsubSubscription = createPubsubSubscription(projectId, pubsubTopic!!)
                    if (pubsubSubscription == null) {
                        fail(
                                "Cannot create Pub/Sub subscription for topic ${projectId}/${pubsubTopic}"
                        )
                    }
                    pubsubSubscriptions.add(pubsubSubscription)
                    pubsubSubscriptionField.setter.call(testInstance, pubsubSubscription)
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun afterAll(context: ExtensionContext) {
        logger.info("Cleaning up the Pub/Sub resources...")
        val testInstance = context.getTestInstance().get()
        val projectId = Utils.readInstanceProperty<String>(testInstance, "projectId")

        val noCleanOptionPubSub = System.getenv("NO_CLEANUP_PUBSUB")
        if (noCleanOptionPubSub == null || noCleanOptionPubSub != "1") {
            val pubsubSubscriptions =
                    context.getStore(STORE).get(PUBSUB_SUBSCRIPTION) as List<String>
            pubsubSubscriptions.forEach { deletePubsubSubscription(projectId, it) }

            val pubsubTopics = context.getStore(STORE).get(PUBSUB_TOPIC) as List<String>
            pubsubTopics.forEach { deletePubsubTopic(projectId, it) }
        } else {
            logger.info("!!!NO_CLEANUP_PUBSUB requested. The resources must be deleted manually")
        }
    }
}
