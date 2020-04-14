package com.kafka.streams.tweets;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;
import com.kafka.streams.tweets.config.ConfigConstants;

public class StreamFilterTweets {

	public static void main(String[] args) {
		Properties properties = createProperties();

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> inputTopic = streamsBuilder.stream(ConfigConstants.topic);
		KStream<String, String> filterStream = inputTopic.filter((k, jsonTweets) -> extractFollowersFromTweets(jsonTweets)>10000);
		filterStream.to("important_tweets");
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		kafkaStreams.start(); 
	}

	private static Properties createProperties() {
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigConstants.bootstrapServer);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, ConfigConstants.streamId);
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		return properties;
	}

	private static Integer extractFollowersFromTweets(String tweetJson) {
		JsonParser jsonParser = new JsonParser();
		try {
			return jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
					.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}

}
