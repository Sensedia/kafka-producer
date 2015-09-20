package com.sensedia.kafkapub;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MetricProducer {

	private Integer events;

	public static void main(String[] args) {
		MetricProducer metricProducer = new MetricProducer();
		metricProducer.events = 10;
		metricProducer.producer();
	}

	public void producer() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "broker1:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.sensedia.kafkapub.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		KeyedMessage<String, String> data = new KeyedMessage<String, String>("analytics", "Message_" + events);
		System.out.println("Enviando ...");
		producer.send(data);
		System.out.println("Sucesso!!!");
		producer.close();
	}
}
