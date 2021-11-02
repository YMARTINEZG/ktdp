package com.sigom.ktdp.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DepartmentFigure {
    @JsonProperty("TotalSalary")
    private Integer totalSalary;
    @JsonProperty("EmployeeCount")
    private Integer employeeCount;
    @JsonProperty("AverageSalary")
    private Double averageSalary;
}
