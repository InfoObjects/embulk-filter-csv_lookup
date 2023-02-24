package org.embulk.filter.csv_lookup;

import com.google.common.base.Optional;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class CsvLookupFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(CsvLookupFilterPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("mapping_from")
        public List<String> getMappingFrom();

        @Config("mapping_to")
        public List<String> getMappingTo();

        @Config("new_columns")
        public SchemaConfig getNewColumns();

        @Config("path_of_lookup_file")
        public String getPathOfLookupFile();

    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        List<String> inputColumns = task.getMappingFrom();
        List<String> keyColumns = task.getMappingTo();
        if(inputColumns.size()!=keyColumns.size()){
            throw new RuntimeException("Number of mapping_from columns must be exactly equals to number of mapping_to columns");
        }

        Schema outputSchema = inputSchema;

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (Column inputColumn : inputSchema.getColumns()) {
            Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }

        for (ColumnConfig columnConfig : task.getNewColumns().getColumns()) {
            builder.add(columnConfig.toColumn(i++));
        }
        outputSchema = new Schema(builder.build());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Map<String, List<String>> map = new HashMap<>();
        try {
            try {
                map = getKeyValueMap(task);
            } catch (CsvValidationException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        PageReader pageReader = new PageReader(inputSchema);
        return new MyOutput(pageReader, inputSchema, outputSchema, output, task, map);
    }
    private Map<String, List<String>> getKeyValueMap(PluginTask task) throws SQLException, IOException, CsvValidationException, CsvValidationException {
        Map<String, List<String>> map = new LinkedHashMap<>();

        List<String> targetColumns = task.getMappingTo();
        List<String> newColumns = new ArrayList<>();

        for (ColumnConfig columnConfig : task.getNewColumns().getColumns()) {
            newColumns.add(columnConfig.getName());
        }
        BufferedReader reader = null;
        String line = "";
        reader = new BufferedReader(new FileReader(task.getPathOfLookupFile()));
        String[] lineDataArray;
        Map<String, Integer> map1 = new LinkedHashMap<>();
        List<Integer> list1 = new ArrayList<>();
        List<Integer> list2 = new ArrayList<>();

        while((line = reader.readLine()) != null) {
            lineDataArray = line.split(",");
            for (int s = 0; s< lineDataArray.length; s++) {
                map1.put(lineDataArray[s], s);
            }
            break;
        }

        for (int x = 0; x< targetColumns.size(); x++){
            if (!map1.containsKey(targetColumns.get(x))){
                throw new RuntimeException("Target Columns Not Found!!");
            }
            list1.add(map1.get(targetColumns.get(x)));
        }

        for (int x = 0; x< newColumns.size(); x++){
            if (!map1.containsKey(newColumns.get(x))){
                throw new RuntimeException("New Columns field Not Found!!");
            }
            list2.add(map1.get(newColumns.get(x)));
        }

        CSVReader reader1 = new CSVReader(new FileReader(task.getPathOfLookupFile()));
        String [] nextLine;
        int i = 0;
        while ((nextLine = reader1.readNext()) != null) {
            if (i!=0){

                //for Key
                String key = "";
                for (int z = 0; z< list1.size(); z++) {
                    key += nextLine[list1.get(z)];
                    if (z != list1.size() - 1) {
                        key += ",";
                    }
                }

                //for Values
                List<String> keyArray = new ArrayList<>();
                for (int z = 0; z < newColumns.size(); z++) {
                    keyArray.add(nextLine[list2.get(z)]);
                }
                map.put(key, keyArray);
            }i++;
        }
        return map;
    }

    public static class MyOutput implements PageOutput {
        private PageReader reader;
        private PageBuilder builder;
        private PluginTask task;
        private Schema inputSchema;
        private Map<String, List<String>> keyValuePair;

        public MyOutput(PageReader pageReader, Schema inputSchema, Schema outputSchema, PageOutput pageOutput, PluginTask task, Map<String, List<String>> keyValuePair) {
            this.reader = pageReader;
            this.builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
            this.task = task;
            this.inputSchema = inputSchema;
            this.keyValuePair = keyValuePair;
        }

        @Override
        public void add(Page page) {
            reader.setPage(page);
            List<ColumnConfig> columnConfigList = new ArrayList<>();
            for (ColumnConfig columnConfig : task.getNewColumns().getColumns()) {
                columnConfigList.add(columnConfig);
            }
            Set<String> unmatchedData = new LinkedHashSet<>();
            List<String> keyColumns = task.getMappingFrom();

            while (reader.nextRecord()) {

                int colNum = 0;
                List<String> inputColumns = task.getMappingFrom();
                List<String> searchingKeyData = new ArrayList<>();
                Map<String, Integer> keyMap = new HashMap<>();
                keyMap.put("Key", 0);

                for (Column column : inputSchema.getColumns()) {
                    if (reader.isNull(column)) {
                        if ((keyMap.get("Key") < inputColumns.size()) && column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                            searchingKeyData.add("");
                            int key = keyMap.get("Key");
                            keyMap.put("Key", ++key);
                        }
                        builder.setNull(colNum++);
                    } else {
                        add_builder(colNum++, column, searchingKeyData, inputColumns, keyMap);
                    }
                }

                String key = "";
                for (int k = 0; k < searchingKeyData.size(); k++) {
                    key += searchingKeyData.get(k);
                    if (k != searchingKeyData.size() - 1) {
                        key += ",";
                    }
                }

                List<String> matchedData = new ArrayList<>();
                if (keyValuePair.containsKey(key)) {
                    matchedData = keyValuePair.get(key);
                }else{
                    unmatchedData.add(key);
                }

                if (matchedData.size() == 0) {
                    for (int k = 0; k < columnConfigList.size(); k++) {
                        add_builder_for_new_column(colNum, columnConfigList.get(k).getType().getName(), "", false);
                        colNum++;
                    }
                } else {
                    for (int k = 0; k < columnConfigList.size(); k++) {
                        add_builder_for_new_column(colNum, columnConfigList.get(k).getType().getName(), matchedData.get(k), true);
                        colNum++;
                    }
                }
                builder.addRecord();
            }
            String info="\n--------------------Unmatched rows.....................\nMapping Key Columns: ";
            for(int i=0;i<keyColumns.size();i++){
                info+= keyColumns.get(i);
                if(i!=keyColumns.size()-1){
                    info+=",";
                }
            }
            info+="\n";

            for(String key: unmatchedData){
                info+= key+"\n";
            }
            logger.info(info);

        }

        @Override
        public void finish() {
            builder.finish();
        }

        @Override
        public void close() {
            builder.close();
        }

        private void add_builder(int colNum, Column column, List<String> searchingKeyData, List<String> inputColumns, Map<String, Integer> keyMap) {
            if (Types.STRING.equals(column.getType())) {
                if (keyMap.get("Key") < inputColumns.size()) {
                    if (column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                        searchingKeyData.add(reader.getString(column));
                        int key = keyMap.get("Key");
                        keyMap.put("Key", ++key);
                    }
                }
                builder.setString(colNum, reader.getString(column));
            } else if (Types.BOOLEAN.equals(column.getType())) {
                if (keyMap.get("Key") < inputColumns.size()) {
                    if (column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                        searchingKeyData.add(String.valueOf(reader.getBoolean(column)));
                        int key = keyMap.get("Key");
                        keyMap.put("Key", ++key);
                    }
                }
                builder.setBoolean(colNum, reader.getBoolean(column));
            } else if (Types.DOUBLE.equals(column.getType())) {
                if (keyMap.get("Key") < inputColumns.size()) {
                    if (column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                        searchingKeyData.add(String.valueOf(reader.getDouble(column)));
                        int key = keyMap.get("Key");
                        keyMap.put("Key", ++key);
                    }
                }
                builder.setDouble(colNum, reader.getDouble(column));
            } else if (Types.LONG.equals(column.getType())) {
                if (keyMap.get("Key") < inputColumns.size()) {
                    if (column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                        searchingKeyData.add(String.valueOf(reader.getLong(column)));
                        int key = keyMap.get("Key");
                        keyMap.put("Key", ++key);
                    }
                }

                builder.setLong(colNum, reader.getLong(column));
            } else if (Types.TIMESTAMP.equals(column.getType())) {
                if (keyMap.get("Key") < inputColumns.size()) {
                    if (column.getName().equalsIgnoreCase(inputColumns.get(keyMap.get("Key")))) {
                        searchingKeyData.add(String.valueOf(reader.getTimestamp(column)));
                        int key = keyMap.get("Key");
                        keyMap.put("Key", ++key);
                    }
                }
                builder.setTimestamp(colNum, reader.getTimestamp(column));
            }
        }

        private void add_builder_for_new_column(int colNum, String newlyAddedColumnType, String matchedData, Boolean isDataMatched) {
            try{
                if (newlyAddedColumnType.equalsIgnoreCase("string")) {
                    if (isDataMatched) {
                        builder.setString(colNum, matchedData);
                    } else {
                        builder.setString(colNum, "");
                    }

                } else if (newlyAddedColumnType.equalsIgnoreCase("long")) {
                    if (isDataMatched) {
                        if (matchedData.length() == 0) {
                            builder.setLong(colNum, 0);
                        }else{
                            builder.setLong(colNum, Long.parseLong(matchedData));
                        }
                    } else {
                        builder.setLong(colNum, 0);
                    }

                } else if (newlyAddedColumnType.equalsIgnoreCase("double")) {
                    if (isDataMatched) {
                        if (matchedData.length() == 0) {
                            builder.setDouble(colNum, 0.0);
                        }else{
                            builder.setDouble(colNum, Double.parseDouble(matchedData));
                        }
                    } else {
                        builder.setDouble(colNum, 0.0);
                    }
                } else if (newlyAddedColumnType.equalsIgnoreCase("boolean")) {
                    if (isDataMatched) {
                        if (matchedData.length() == 0) {
                            builder.setNull(colNum);
                        }else{
                            builder.setBoolean(colNum, Boolean.parseBoolean(matchedData));
                        }
                    } else {
                        builder.setNull(colNum);
                    }
                } else if (newlyAddedColumnType.equalsIgnoreCase("timestamp")) {
                    if (isDataMatched) {
                        if (matchedData.length() == 0) {
                            builder.setNull(colNum);
                        }else{
                            java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(matchedData);
                            Instant instant = timestamp.toInstant();
                            Timestamp spiTimeStamp = Timestamp.ofInstant(instant);
                            builder.setTimestamp(colNum, spiTimeStamp);
                        }
                    } else {
                        builder.setNull(colNum);
                    }

                }
            }catch (Exception e){
                e.printStackTrace();
                throw new RuntimeException("Data type could not be cast due to wrong data or issue in typecasting timestamp",e);
            }

        }

    }
}
