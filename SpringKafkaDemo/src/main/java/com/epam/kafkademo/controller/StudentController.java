package com.epam.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.epam.kafkademo.dto.Student;
import com.epam.kafkademo.service.StudentService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/studentapi")
public class StudentController {
	
	private KafkaTemplate<String,Student> kafkaTemplate;
	@Autowired
	StudentService studentService;
	
	public StudentController(KafkaTemplate<String, Student> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@PostMapping
	public Mono<Student> publishStudent(@RequestBody Student student) {
		
		kafkaTemplate.send("student", student);
		
		return Mono.just(student);
	}
	
	@GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<String> getStudents(){
		return studentService.getStudents();
	}
}
