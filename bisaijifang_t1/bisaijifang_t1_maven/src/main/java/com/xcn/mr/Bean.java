package com.xcn.mr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable {

    // {"result": {
    // "公司名称": "阿里巴巴",
    // "工作城市": "广州",
    // "工作要求": "岗位职责：1、负责公司司法事业线系统规划，完成产品线需求规划、功能业务规划、流程设计、产品设计、评估/优化、版本演进，达成产品目标;2、制定详细的产品计划并跟踪好执行情况，把握产品的关键点和里程碑，规避产品风险，按质按量完成项目；3、解决和协调解决产品当中技术问题；4、与客户沟通，解决客户提出各类问题和发掘客户的潜在需求；5、把客户的需求整理成相应的文档，能编写产品方案文档；岗位要求：1、本科以上学历、具备3年以上业务软件系统的需求分析经验，有物联网相关经验者优先；2、具备良好的沟通协调能力，思维逻辑清晰、能独立访谈客户了解需求；3、具备需求分析常用工具使用技能：visio/Axure RP/office/思维导图；4、良好的文字表达与文字理解能力、至少独立负责完成两个以上项目的项目解决方案、需求调研，需求分析、原型设计、需求规格说明书经验；5、有政府、事业单位软件需求分析经验优先；6、可接受省内短期出差。职能类别：需求工程师软件工程师关键字：需求设计分析系统分析五险一金年终奖金双休微信分享",
    // "招聘人数": 14,
    // "工资情况": "5000-10000",
    // "name": "产品经理",
    // "detail": "岗位职责：1、负责公司司法事业线系统规划，完成产品线需求规划、功能业务规划、流程设计、产品设计、评估/优化、版本演进，达成产品目标;2、制定详细的产品计划并跟踪好执行情况，把握产品的关键点和里程碑，规避产品风险，按质按量完成项目；3、解决和协调解决产品当中技术问题；4、与客户沟通，解决客户提出各类问题和发掘客户的潜在需求；5、把客户的需求整理成相应的文档，能编写产品方案文档；岗位要求：1、本科以上学历、具备3年以上业务软件系统的需求分析经验，有物联网相关经验者优先；2、具备良好的沟通协调能力，思维逻辑清晰、能独立访谈客户了解需求；3、具备需求分析常用工具使用技能：visio/Axure RP/office/思维导图；4、良好的文字表达与文字理解能力、至少独立负责完成两个以上项目的项目解决方案、需求调研，需求分析、原型设计、需求规格说明书经验；5、有政府、事业单位软件需求分析经验优先；6、可接受省内短期出差。职能类别：需求工程师软件工程师关键字：需求设计分析系统分析五险一金年终奖金双休微信分享"}},

    private String company_name;
    private String city;
    private String require;
    private String man;
    private String salary;
    private String name;
    private String detail;


    @Override
    public String toString() {
        return company_name + '|'+ city + '|' + require + '|' + man + '|' + salary + '|'  + name + '|' + detail ;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(company_name);
        out.writeUTF(city);
        out.writeUTF(require);
        out.writeUTF(man);
        out.writeUTF(salary);
        out.writeUTF(name);
        out.writeUTF(detail);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        company_name = in.readUTF();
        city = in.readUTF();
        require = in.readUTF();
        man = in.readUTF();
        salary = in.readUTF();
        name = in.readUTF();
        detail = in.readUTF();
    }
    public Bean(){

    }
    public Bean(String company_name, String city, String require, String man, String salary, String name, String detail) {
        this.company_name = company_name;
        this.city = city;
        this.require = require;
        this.man = man;
        this.salary = salary;
        this.name = name;
        this.detail = detail;
    }

    public void setAll(String company_name, String city, String require, String man, String salary, String name, String detail){
        this.company_name = company_name;
        this.city = city;
        this.require = require;
        this.man = man;
        this.salary = salary;
        this.name = name;
        this.detail = detail;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setRequire(String require) {
        this.require = require;
    }

    public void setMan(String man) {
        this.man = man;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public String getCompany_name() {
        return company_name;
    }

    public String getCity() {
        return city;
    }

    public String getRequire() {
        return require;
    }

    public String getMan() {
        return man;
    }

    public String getSalary() {
        return salary;
    }

    public String getName() {
        return name;
    }

    public String getDetail() {
        return detail;
    }
}

