package com.sigom.ktdp.services;

import com.sigom.ktdp.model.DepartmentFigure;
import com.sigom.ktdp.model.Employee;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

@Configuration
@EnableAutoConfiguration
@Log4j2
public class EmployeeListenerService {

    @Bean
    public Function<KStream<String, String>,KStream<String, String>> process() {
        return input -> input.mapValues(v -> v.toUpperCase());
    }

//    private DepartmentFigure init(Employee employee){
//        DepartmentFigure department = new DepartmentFigure();
//        department.setEmployeeCount(1);
//        department.setTotalSalary(employee.getSalary());
//        department.setAverageSalary(0D);
//        return department;
//    }
//    private String initWorld(String world){
//        return world.toUpperCase();
//    }
}
