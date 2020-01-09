package de.npruehs.missionrunner.analytics.producer;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnalyticsController {
	private Producer<String, String> producer;
	
	@PostConstruct
	public void postConstruct() {
	    // Setup Kafka producer.
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
	}
	
	@PostMapping("/analytics/post")
	public void post(@RequestBody AnalyticsEvent event) {
		if (event.getEventId() == null || event.getEventId().isEmpty()) {
			throw new IllegalArgumentException("Missing event id.");
		}
		
		producer.send(new ProducerRecord<String, String>("missionrunner", null, event.getEventId()));
	}
	
	@PreDestroy
    public void preDestroy() throws Exception {
        producer.close();
    }
}
