package com.SDEadda247.elasticsearch.service;

import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.io.Serializable;

@Service
@Configuration
public class service implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7305965478911631142L;
	JestClient client = null;

	public JestClient jestClient() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("https://search-ytsearch-staging-vflomzxcm3c4pklej6nwyomxfm.us-east-1.es.amazonaws.com/").multiThreaded(true)
				.defaultMaxTotalConnectionPerRoute(2).maxTotalConnection(10).build());
		return factory.getObject();
	}

}
