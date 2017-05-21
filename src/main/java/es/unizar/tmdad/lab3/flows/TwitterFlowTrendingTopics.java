package es.unizar.tmdad.lab3.flows;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.social.twitter.api.Tweet;

@Configuration
@Profile("fanout")
public class TwitterFlowTrendingTopics extends TwitterFlowCommon {

	final static String TRENDING_TOPICS_QUEUE_NAME = "trending_topics_queue";

	@Autowired
	private FanoutExchange fanoutExchange;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Bean
	public Queue trendingTopicsQueue() {
		return new Queue(TRENDING_TOPICS_QUEUE_NAME, false, false, false);
	}

	@Bean
	public Binding twitterFanoutBinding() {
		return BindingBuilder.bind(trendingTopicsQueue()).to(fanoutExchange);
	}

	@Override
	@Bean
	public DirectChannel requestChannelRabbitMQ() {
		return MessageChannels.direct().get();
	}

	@Bean
	public DirectChannel requestChannelTwitter() {
		return MessageChannels.direct().get();
	}

	@Bean
	public AmqpInboundChannelAdapter amqpInbound() {
		SimpleMessageListenerContainer simpleMessageListenerContainer = new SimpleMessageListenerContainer(rabbitTemplate.getConnectionFactory());
		simpleMessageListenerContainer.setQueues(trendingTopicsQueue());
		return Amqp.inboundAdapter(simpleMessageListenerContainer).outputChannel(requestChannelRabbitMQ()).get();
	}

	@Bean
	public IntegrationFlow sendTrendingTopics() {
		return IntegrationFlows.from(requestChannelTwitter())
				.filter((Object object) -> object instanceof Tweet)
				.aggregate(aggregationSpec(), null)
				.<List<Tweet>, List<Map.Entry<String, Integer>>>transform(tweetList -> {
					Map<String, Integer> trendingTopics = new HashMap<String, Integer>();
					tweetList.stream().forEach(tweet -> {
						tweet.getEntities().getHashTags().forEach(hashtag-> {
							if (trendingTopics.containsKey(hashtag.getText())) trendingTopics.put(hashtag.getText(), trendingTopics.get(hashtag.getText())+1);
							else trendingTopics.put(hashtag.getText(), 1);
						});
					});
					Comparator<Map.Entry<String, Integer>> valueComparator = (Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> Integer.compare(e2.getValue(), e1.getValue());
					return trendingTopics.entrySet().stream()
                        .sorted(valueComparator)
                        .limit(10)
                        .collect(Collectors.toList());
				})
				.handle("streamSendingService", "sendTrends").get();
	}
	
	private Consumer<AggregatorSpec> aggregationSpec() {
		return a -> a.correlationStrategy(m -> 1)
				.releaseStrategy(g -> g.size() == 1000)
				.expireGroupsUponCompletion(true);
	}
}