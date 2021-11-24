package dataflow.e2e.demo

import com.google.api.services.bigquery.model.TableRow
import java.util.stream.Stream
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.TupleTagList
import org.junit.Assert.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.LoggerFactory

class AppTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("providesParseJsonMessageTestValues")
    fun parseJsonMessageTest(
            desc: String,
            message: String,
            expected1: List<TableRow>,
            expected2: List<TableRow>
    ) {
        logger.info("Run Test {}...", desc)
        val p: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)
        val input = p.apply(Create.of(message)).setCoder(StringUtf8Coder.of())
        val output =
                input.apply(
                        ParDo.of(DemoParseJsonMessage())
                                .withOutputTags(
                                        DemoParseJsonMessage.OUTPUT_TAG,
                                        TupleTagList.of(DemoParseJsonMessage.OUTPUT2_TAG)
                                )
                )

        PAssert.that(output.get(DemoParseJsonMessage.OUTPUT_TAG)).containsInAnyOrder(expected1)
        PAssert.that(output.get(DemoParseJsonMessage.OUTPUT2_TAG)).containsInAnyOrder(expected2)

        p.run()
    }

    companion object {
        @JvmStatic val logger = LoggerFactory.getLogger(AppTest::class.java)

        @JvmStatic
        fun providesParseJsonMessageTestValues(): Stream<Arguments> =
                Stream.of(
                        Arguments.of(
                                "valid message - type 1",
                                """{
                                    "type": "1",
                                    "data":"data for type 1"
                                }""",
                                listOf(TableRow().set("type", "1").set("data", "data for type 1")),
                                listOf<TableRow>()
                        ),
                        Arguments.of(
                                "valid message - type 2",
                                """{
                                    "type": "2",
                                    "data":"data for type 2"
                                }""",
                                listOf<TableRow>(),
                                listOf(TableRow().set("type", "2").set("data", "data for type 2"))
                        ),
                )
    }
}
