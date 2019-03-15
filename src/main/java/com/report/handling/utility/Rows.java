package com.report.handling.utility;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by yyeruva on 14-03-2019.
 */
public class Rows {

    public Rows(List<Columns> columnsList) {
        this.columnsList = columnsList;
    }

    List<Columns> columnsList = new LinkedList<>();

    public void add(Columns columns) {
        columnsList.add(columns);
    }

    public List getContent() {
        return columnsList;
    }
}
