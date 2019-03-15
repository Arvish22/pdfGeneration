package com.report.handling.utility;

import java.util.ArrayList;
import java.util.List;

public class Columns {

    /*private List rows = new ArrayList();*/

    /*private List<String> stringList;


    public Data() {
        stringList = new LinkedList<>();
    }

    public Data(List<String> stringList) {
        this.stringList = stringList;
    }

    public List<String> getStringList() {
        return stringList;
    }

    public void setStringList(List<String> stringList) {
        this.stringList = stringList;
    }

    public boolean addData(String data) {
        return stringList.add(data);
    }*/
    private Object id, firstname, middlename, lastname, clientname, orgname, orgid, managername, leadname, pin, city, country, longlong;

    public Columns(Object id, Object firstname, Object middlename, Object lastname, Object clientname, Object orgname, Object orgid, Object managername, Object leadname, Object pin, Object city, Object country, Object longlong) {
        this.id = id;
        this.firstname = firstname;
        this.middlename = middlename;
        this.lastname = lastname;
        this.clientname = clientname;
        this.orgname = orgname;
        this.orgid = orgid;
        this.managername = managername;
        this.leadname = leadname;
        this.pin = pin;
        this.city = city;
        this.country = country;
        this.longlong = longlong;
    }

}
