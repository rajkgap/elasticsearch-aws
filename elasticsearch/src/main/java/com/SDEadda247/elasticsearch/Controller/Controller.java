package com.SDEadda247.elasticsearch.Controller;

import java.io.IOException;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.SDEadda247.elasticsearch.user.user;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class Controller {
	
    JestClient client =null;
    public JestClient getClient() {
        if(this.client==null)
        {
            System.out.println("setting up connection with jedis");
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(
                    new HttpClientConfig.Builder("https://search-ytsearch-staging-vflomzxcm3c4pklej6nwyomxfm.us-east-1.es.amazonaws.com")
                            .multiThreaded(true)
                            .defaultMaxTotalConnectionPerRoute(2)
                            .maxTotalConnection(10)
                            .build());
            this.client=factory.getObject();
            return factory.getObject();
        }
        return this.client;

    }
    
    
    @PostMapping("/save")
    public String saveUser(@RequestBody user User) throws IOException {
    	JestClient client = this.getClient();     
    	ObjectMapper mapper = new ObjectMapper();
    	JsonNode usernode=mapper.createObjectNode()
    			.put("name",User.getName())
    			.put("email",User.getEmail())
    			.put("phone",User.getPhone());
    	
    	JestResult Result = client.execute(new Index.Builder(usernode.toString()).index("raj_user").type("user").build());
    	return Result.toString();

    }
    
    @GetMapping("/find/{id}")
    public String findUser(@PathVariable final String id) throws IOException {
        JestClient client = this.getClient();

        JestResult Result = client.execute(new Get.Builder("raj_user",id).type("user").build());
        return Result.toString();
    }
    
    @PutMapping("/update/{id}")
    public String updateUser(@PathVariable final String id ,@RequestBody user User) throws IOException
    {
        JestClient client = this.getClient();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode userNode = mapper.createObjectNode()
    			.put("name",User.getName())
    			.put("email",User.getEmail())
    			.put("phone",User.getPhone());
        JestResult Result  = client.execute(new Update.Builder(userNode.toString()).index("raj_user").type("user").id(id).build());

        return Result.toString();
    }
    
    @DeleteMapping("/delete/{id}")
    public String deleteUser(@PathVariable final String id, @RequestBody user User )throws IOException
    {
        JestClient client = this.getClient();

        JestResult Result = client.execute(new Delete.Builder(id).index("raj_user").type("user").build());
        return Result.toString();
    }
    
    

}
