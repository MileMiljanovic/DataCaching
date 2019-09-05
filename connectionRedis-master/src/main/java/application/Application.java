package application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"web.controller","application","redis.connection"} )
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);

	}
	
//	@EnableCircuitBreaker
//	This annotation is applied on Java classes that can act as the circuit breaker. The circuit breaker pattern can allow a micro service continue working when a related service fails, preventing the failure from cascading. This also gives the failed service a time to recover.
//
//	The class annotated with @EnableCircuitBreaker will monitor, open, and close the circuit breaker.
//
//	@HystrixCommand
//	This annotation is used at the method level. Netflix’s Hystrix library provides the implementation of Circuit Breaker pattern. When you apply the circuit breaker to a method, Hystrix watches for the failures of the method. Once failures build up to a threshold, Hystrix opens the circuit so that the subsequent calls also fail. Now Hystrix redirects calls to the method and they are passed to the specified fallback methods.
//	Hystrix looks for any method annotated with the @HystrixCommand annotation and wraps it into a proxy connected to a circuit breaker so that Hystrix can monitor it.
//
//	Consider the following example:
//
//	@Service
//	public class BookService{
//	    private final RestTemplate restTemplate; 
//	    public BookService(RestTemplate rest){
//	      this.restTemplate =   rest;
//	    }                                           
//	  @HystrixCommand(fallbackMethod = "newList")                                                                     public String bookList(){
//	    URI uri = URI.create("http://localhost:8081/recommended");                                                      return this.restTemplate.getForObject(uri, String.class);  
//	  }
//	  public String newList(){
//	    return "Cloud native Java";
//	  }
//	}
//	Here @HystrixCommand is applied to the original method bookList(). The @HystrixCommand annotation has newList as the fallback method. So for some reason if Hystrix opens the circuit on bookList(), you will have a placeholder book list ready for the users.
}
