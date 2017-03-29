// TODO add licence

package org.example.wordcount;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline that reads JSON tweets from a Kafka topic and counts words in all the tweets
 * in 2 minute fixed windows. Format of the the tweets is specified at
 * https://dev.twitter.com/overview/api/tweets. The word counts are written back to another
 * Kafka topic. <p>
 *
 * The maven configuration is based on Beam quickstart at
 * https://beam.apache.org/get-started/quickstart-java/ <br>
 *
 * To run it on direct runner (see quickstart page for other options:
 * <pre>
 *  $ mvn compile exec:java -Dexec.mainClass=org.example.wordcount.StreawmingWordCount
 * </pre>
 */
public class StreamingWordCount {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWordCount.class);

  /**
   * Options for the app.
   */
  public static interface Options extends PipelineOptions, StreamingOptions {
    @Description("Fixed window length in minutes")
    @Default.Integer(2)
    Integer getFixedWindowLengthMinutes();
    void setFixedWindowLengthMinutes(Integer value);

    @Description("Bootstrap Server(s) for Kafka")
    @Default.String("localhost:9092")
    String getBootstrapServers();
    void setBootstrapServers(String servers);

    @Description("Kafka topic to read tweets from")
    @Default.String("tweets")
    String getTopic();
    void setTopic(String topics);

    @Description("Kafka topic name for writing results")
    @Default.String("tweet_word_counts")
    String getOutputTopic();
    void setOutputTopic(String topic);
  }

  public static void main(String args[]) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(KafkaIO.<String, String>read()
                   .withBootstrapServers(options.getBootstrapServers())
                   .withTopics(ImmutableList.of(options.getTopic()))
                   .withKeyCoder(StringUtf8Coder.of()) // Key is actually ignored.
                   .withValueCoder(StringUtf8Coder.of())
                   .withTimestampFn(TWEET_TIMESTAMP_OR_NOW)
                   // Watermark should ideally come source (e.g. based on Kafka server side
                   // timestamp). Here we just use a simple heuristic now - 2 minutes. If the
                   // pipeline lags this could make all the records late.
                   .withWatermarkFn(kv -> Instant.now().minus(Duration.standardMinutes(2)))
                   .withoutMetadata())
        .apply(Values.create()) // This drops key (key is null for the kafka records)
        .apply(ParDo.of(new ExtractMentionsFn()))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(
            options.getFixedWindowLengthMinutes()))))
        .apply(Count.perElement())
            // .apply(Top.of(10, new KV.OrderByValue<String, Long>()).withoutDefaults())
        .apply(ParDo.of(new OutputFormatter()))
        // write output json to a Kafka topic
        .apply(KafkaIO.<String, String>write()
                   .withBootstrapServers(options.getBootstrapServers())
                   .withTopic(options.getOutputTopic())
                   .withKeyCoder(StringUtf8Coder.of()) // Key coder should not be needed writing just values, I will fix.
                   .withValueCoder(StringUtf8Coder.of())
                   .values());

    pipeline.run();
  }

  // The rest of the file implements DoFns used above.

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  // extract timestamp from "timestamp_ms" field.
  private static final SerializableFunction<KV<String, String>, Instant> TWEET_TIMESTAMP_OR_NOW =
      new SerializableFunction<KV<String, String>, Instant>() {
        @Override
        public Instant apply(KV<String, String> kv) {
          try {
            long tsMillis = JSON_MAPPER.readTree(kv.getValue()).path("timestamp_ms").asLong();
            return tsMillis == 0 ? Instant.now() : new Instant(tsMillis);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

  /**
   * Emit each of the users mentioned, if any.
   */
  private static class ExtractMentionsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
      for (JsonNode hashtag : JSON_MAPPER.readTree(ctx.element())
          .with("entities")
          .withArray("user_mentions")) {
        ctx.output(hashtag.get("screen_name").asText());
      }
    }
  }

  // return json string containing word, count, and window information.
  private static class OutputFormatter extends DoFn<KV<String, Long>, String> {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat
        .forPattern("yyyy-MM-dd HH:mm:ss")
        .withZoneUTC();
    private static final ObjectWriter JSON_WRITER = new ObjectMapper()
        .writerFor(OutputJson.class);

    static class OutputJson {
      @JsonProperty String windowStart;
      @JsonProperty String windowEnd;
      @JsonProperty String generatedAt;
      @JsonProperty String word;
      @JsonProperty long count;

      OutputJson(String windowStart, String windowEnd,
                 String generatedAt, String word, long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.generatedAt = generatedAt;
        this.word = word;
        this.count = count;
      }
    }

    @ProcessElement
    public void processElement(ProcessContext ctx, IntervalWindow window) throws Exception {

      String json = JSON_WRITER.writeValueAsString(new OutputJson(
          DATE_FORMATTER.print(window.start()),
          DATE_FORMATTER.print(window.end()),
          DATE_FORMATTER.print(Instant.now()),
          ctx.element().getKey(),
          ctx.element().getValue()));

      ctx.output(json);
    }
  }
}
