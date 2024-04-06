package customPipelines;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.*;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DynamicDataflowPipeline {
    // env variables dont work when deployed to gcp
    // fix using pipeline options arguments
    static Dotenv dotenv = Dotenv.load();
    static String projectId = dotenv.get("PROJECT_ID");
    static String datasetId = dotenv.get("GBQ_DATASET");
    static String subscription = dotenv.get("PUBSUB_SUBSCRIPTION");
    static String invalidPubsubTopic = dotenv.get("INVALID_PUBSUB_TOPIC");

    static final TupleTag<String> createTable = new TupleTag<String>(){};
    static final TupleTag<String> insertTable = new TupleTag<String>(){};
    static final TupleTag<String> directInsert = new TupleTag<String>(){};
    static final TupleTag<String> modifyTable = new TupleTag<String>(){};
    static final TupleTag<String> invalidCategory = new TupleTag<String>(){};

    static LRUCache<TableId, Schema> cache = new LRUCache<>(50);

    static HashMap<Class<?>, StandardSQLTypeName> typeMapping = new HashMap<>();

    static {
        typeMapping.put(String.class, StandardSQLTypeName.STRING);
        typeMapping.put(Long.class, StandardSQLTypeName.INT64);
        typeMapping.put(Integer.class, StandardSQLTypeName.INT64); // Integers can be stored as INT64 in BigQuery
        typeMapping.put(Double.class, StandardSQLTypeName.FLOAT64);
        typeMapping.put(Boolean.class, StandardSQLTypeName.BOOL);
        typeMapping.put(java.sql.Timestamp.class, StandardSQLTypeName.TIMESTAMP);
        typeMapping.put(ArrayList.class, StandardSQLTypeName.ARRAY);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDataflowPipeline.class);

    public static void main(String[] args) {
        System.out.println("Please work IJN");

        Options options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runDynamicDataflowPipeline(options);
    }

    public interface Options extends PipelineOptions {
        boolean isStreaming();
        void setStreaming(boolean value);


        @Default.String("")
        String getInputSubscription();
        void setInputSubscription(String inputSubscription);

        @Default.String("")
        String getDestinationDataset();
        void setDestinationDataset(String destinationDataset);

        @Default.String("")
        String getInvalidPubsubTopic();
        void setInvalidPubsubTopic(String invalidPubsubTopic);

        @Default.String("")
        String getProjectId();
        void setProjectId(String projectId);

        /* create the following custom options
        * 1. inputSubscription
        * 2. destinationDataset
        * 3. invalidPubsubTopic
        * 4. projectID ??*/
    }

    static void runDynamicDataflowPipeline(Options options) {
        Pipeline p = Pipeline.create(options);
        options.setStreaming(true);

        // prioritize CLI passed argument, env variables only useful for local build
        String activeSubscription = (options.getInputSubscription()!=null && !options.getInputSubscription().isEmpty()) ? options.getInputSubscription() : subscription;
        String activeDatasetId = (options.getDestinationDataset()!=null && !options.getDestinationDataset().isEmpty()) ? options.getDestinationDataset() : datasetId;
        String activeInvalidPubSubTopic = (options.getInvalidPubsubTopic()!=null && !options.getInvalidPubsubTopic().isEmpty()) ? options.getInvalidPubsubTopic() : invalidPubsubTopic;
        String activeProjectId = (options.getProjectId()!=null && !options.getProjectId().isEmpty()) ? options.getProjectId() : projectId;

        // too lazy to rename all the dependencies for projectId
        // projectId = (options.getProjectId()!=null && !options.getProjectId().isEmpty()) ? options.getProjectId() : projectId; // might be behaving weird, returning null values

        String fullSubcriptionName = "projects/"+activeProjectId+"/subscriptions/"+activeSubscription;

        // System.out.println(fullSubcriptionName);
        String params = "active parameters: \nprojectId: " + activeProjectId + "\ninputSubscription: " + activeSubscription + "\ndestinationDataset: " + activeDatasetId + "\ninvalidPubsubTopic: " + activeInvalidPubSubTopic ;
        LOG.info(params);


        /* UPDATE THIS
         * Step 1: Read from pubsub
         * Step 2: convert to ByteString
         * Step 3: convert to Json or map
         * Step 4: look at json data and output taggegtuple of (branch, object data) branch values are createTable, insert, modifyTable, and invalidData
         * Step 5:
         *
         *
         */
        PCollection<PubsubMessage> messages = p.apply("ReadFromPubSub", PubsubIO.readMessagesWithAttributes().fromSubscription(fullSubcriptionName));

        PCollectionTuple branches = messages
                .apply(
                "PubsubMessageToString", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String message = new String(c.element().getPayload());
                        c.output(message);
                    }
                })
                )
                .apply("InsertOrCreateTableBranch",
                        /*ParDo.of(new DoFn<String, Map<String, String>>()*/
                        ParDo.of(new DoFn<String, String>()
                        {
                            @ProcessElement
                            public void processElement(ProcessContext context) throws Exception {

                                String message = context.element();
                                String tableName = HelperFunctions.extractTableName(message);
                                /*if (true) {*/ // testing
                                // if table exists in Cache
                                boolean isTableInCache = HelperFunctions.doesTableExistInCache(TableId.of(activeDatasetId, tableName));
                                boolean tableExists;

                                if (isTableInCache) {
                                    System.out.println("Table: " + tableName + " found in  Cache"); // change to debug log
                                    tableExists = true;
                                } else {
                                    LOG.info("Table: " + tableName + " NOT found in Cache, checking BQ");
                                    // if table exists in BQ
                                    tableExists = HelperFunctions.doesTableExist(activeProjectId, activeDatasetId, tableName);

                                    // if true add table to cache
                                    if (tableExists) {
                                        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                                        Table table = bigquery.getTable(TableId.of(activeDatasetId, tableName));
                                        Schema schemaFromBigQuery = HelperFunctions.getTableSchema(table);

                                        cache.put(TableId.of(activeDatasetId, tableName), schemaFromBigQuery);
                                        LOG.info("Table: " + tableName + " exists in BQ and added to Cache, no need to create");
                                    }
                                }

                                if (tableExists) {
                                    JsonObject data = HelperFunctions.convertPubsubtoJsonObject(message);
                                    // consider adding table name to data here
                                    data = HelperFunctions.addTableName(data);
                                    // context.output(insertTable, message);
                                    context.output(insertTable, data.toString());
                                } else {
                                    context.output(createTable, message);
                                }
                            }
                        }).withOutputTags(insertTable, TupleTagList.of(createTable))
                );

            branches.get(createTable).apply(
                    "CreateTable", ParDo.of(new /*DoFn<String, String>*/DoFn<String, String> () {
                        @ProcessElement
                        public void processElement(ProcessContext c) throws Exception {
                            String message = c.element();
                            System.out.println("Creating Table: " + HelperFunctions.extractTableName(c.element())); // change to debug logging

                            // JsonObject attempt
                            JsonObject jsonMessage = HelperFunctions.convertPubsubtoJsonObject(message);
                            JsonObject jsonData = jsonMessage.get("data").getAsJsonObject();

                            String tableName = jsonMessage.get("table_name").getAsString();

                            // System.out.println("table name from jsonMessage: " + tableName); // change to debug logging
                            jsonData.addProperty("table_name", tableName);

                            // generate table schema using the message body
                            // create the table in the data set
                            System.out.println(c.element() + "create Table branch");
                            try {
                                System.out.println(c.element() + "create Table branch");

                                Schema eventSchema = HelperFunctions.createSchemaFromEvent(HelperFunctions.createHashmapFromJsonObject(jsonData));
                                // Schema eventSchema = HelperFunctions.createSchemaFromJsonEvent(jsonData);
                                System.out.println("Event Schema: " + eventSchema); // change to debug logging

                                // create bigquery table
                                BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                                TableId tableId = TableId.of(activeDatasetId, tableName);
                                TableDefinition tableDefinition = StandardTableDefinition.of(eventSchema);
                                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

                                if (HelperFunctions.doesTableExist(activeProjectId, activeDatasetId, tableName)) { // consider overloading to use tableInfo
                                    LOG.warn("Table " + tableId.toString() + " already exists");
                                } else {
                                    bigquery.create(tableInfo); // gets response 409, table already exists if block to help with that
                                    cache.put(tableId, eventSchema);
                                    LOG.info("Table: " + tableName + " created and added to Cache");
                                }
                                // System.out.println("Created Table: " + tableName); // change to debug logging
                                LOG.info("Current table(s) in cache: " + cache);
                                LOG.info("Current cache size is: " + cache.size());
                            } catch (BigQueryException bq) {
                                LOG.warn("BigQueryException Encountered in Create Table Step: ", bq);
                            }
                            catch (Exception e) {
                                LOG.error("General Exception encountered: ", e); // change to logging - error
                            }
                            // c.output(c.element());

                            c.output((jsonData.toString())); // cant output LinkedTreeMap, JsonObject or (JsonObject.getAsString -> UnsupportedOperationException)
                            System.out.println("End of Create Table step, testing code reach " + jsonData.getClass());
                        }
                    })
            ).apply(
                    "CreateTable-ConvertToTableRow",
                    ParDo.of(new DoFn<String, TableRow>() {
                        @ProcessElement
                        public void processContext(ProcessContext c){
                            try {
                                Gson gson = new Gson();
                                String message = c.element();
                                TableRow row = gson.fromJson(message, TableRow.class);
                                System.out.println("data type: " + row.getClass() + "\ndata: " + row.toString());
                                c.output(row);
                            } catch (Exception e) {
                                System.out.println("Exception in Convert to BQRow step: " + e);
                                e.printStackTrace();
                            }

                        }
                    })
            )//;
            .apply(
                    "insertToNewTable",
                    BigQueryIO.writeTableRows()
                            .to((ValueInSingleWindow<TableRow> event) -> {
                                String tableDest;
                                tableDest = event.getValue().get("table_name").toString();
                                System.out.println("table_dest: " + tableDest);

                                TableDestination fullTableDest = new TableDestination(
                                        String.format("%s.%s.%s", activeProjectId, activeDatasetId, tableDest),
                                        "null"
                                );
                                return fullTableDest;
                            })
                            //.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                            .withTriggeringFrequency(Duration.standardSeconds(5)) // when it needs to be available for reads on BQ
                            .withNumStorageWriteApiStreams(5)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withoutValidation()
                            //.withTriggeringFrequency(Duration.standardDays(1))
            );

        PCollectionTuple modifyOrInsert = branches.get(insertTable).apply(
                "Modify or Insert Branching",
                ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws ParseException {

                        /* Verify Schema
                        *   if schema is same, insert
                                *   if schema is subset, insert
                                *   if schema is changed, check for
                        *       data type change, if so, send to dead letter queue
                        *       else, modify schema and create additional columns
                        * */

                        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

                        String element = c.element();
                        JsonObject elementJson = HelperFunctions.convertPubsubtoJsonObject(element);
                        String tableName = elementJson.get("table_name").getAsString();
                        LOG.info("before getting tableID: " + tableName + " projectid: " + activeProjectId + " dataset: " + activeDatasetId);
                        // TableId tableId = TableId.of(projectId, activeDatasetId, tableName); // getting "java.lang.NullPointerException com.google.common.base.Preconditions.checkNotNull" when deployed
                        TableId tableId = TableId.of(activeDatasetId, tableName);

                        // Table table = bigquery.getTable(tableId); // moving to the section that needs it, that is, when table is modified
                        // Schema schemaFromBigQuery = HelperFunctions.getTableSchema(table);
                        Schema schemaFromBigQuery = cache.get(tableId); // from cache

                        HashMap hashMapEvent = HelperFunctions.createHashmapFromJsonObject(elementJson);
                        Schema localSchema = HelperFunctions.createSchemaFromEvent(hashMapEvent);

                        boolean doesSchemaMatch = localSchema.equals(schemaFromBigQuery);
                        boolean isSchemaSubset = schemaFromBigQuery.getFields().containsAll(localSchema.getFields());

                        boolean didDataTypeChange = false;
                        boolean areMoreColumns =  true; /// FIX THIS

                        if (doesSchemaMatch || isSchemaSubset) {
                            LOG.debug("Schema from bq: " + schemaFromBigQuery);
                            LOG.debug("Local Schema  : " + localSchema/*.getFields()*/);

                            // System.out.println("Does Schema match? : " + (localSchema.toString().equals(schemaFromBigQuery.toString())));
                            LOG.debug("Does Schema match? : " + (doesSchemaMatch));
                            // System.out.println("Subset? : " + isSchemaSubset);
                            LOG.debug("Subset? : " + isSchemaSubset);

                            LOG.info("Routing to direct insert");
                            c.output(directInsert, c.element());
                        }

                        if (!doesSchemaMatch && didDataTypeChange) { // might be unreachable because a different data type is a schema mismatch and is essentially a new column
                            LOG.info("Invalid data due to schema and data type mismatch");
                            c.output(invalidCategory, c.element());
                        }

                         if (!doesSchemaMatch && areMoreColumns && !isSchemaSubset) {
                             // FieldList newColumns = schemaFromBigQuery.getFields();
                             // newColumns.removeAll(localSchema.getFields()); // not working
                             // System.out.println("New Column: " + newColumnSet);
                             try {
                                 Set<Field> fromBQ = new HashSet<>(schemaFromBigQuery.getFields());
                                 Set<Field> fromLocal = new HashSet<>(localSchema.getFields());
                                 Set<Field> newColumnSet = Sets.difference(fromLocal, fromBQ); // the order matters

                                 LOG.debug("local schema: " + localSchema.getFields());
                                 LOG.debug("new fields: " + newColumnSet);
                                 // Create a new schema adding the current fields, plus the new one
                                 List<Field> fieldList = new ArrayList<Field>(schemaFromBigQuery.getFields());
                                 fieldList.addAll(newColumnSet);
                                 LOG.debug("new fields list: " + fieldList);
                                 Schema newSchema = Schema.of(fieldList);

                                 LOG.info("Complete new schema with new columns: " + newSchema);

                                 // modify table with the new schema
                                 Table table = bigquery.getTable(tableId);
                                 Table updatedTable =
                                         table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
                                 updatedTable.update();
                                 System.out.println("New column(s) successfully added to table");
                                 // Update cache
                                 cache.replace(tableId, newSchema);
                                 LOG.info("Cache update for Table: " + tableName);

                                 // CAVEAT
                                 // if one column fails then no column is added. A simple case would be when a field is detected as a new column because it has a different datatype.
                                 // it fails because the fields must have unique names, present fields are not overwritten for data integrity
                                 // so other fields are not added. EVEN WITH IMPLICIT CONVERT AT BQ end.
                             } catch (BigQueryException e) {
                                 LOG.error("New column(s) not added. \n" + e);
                             } catch (Exception e) {
                                 LOG.error("Exception when modifying table to add columns", e);
                             }

                             // send event to next step in pipeline.
                             c.output(modifyTable, c.element());
                         }
                    }
                }).withOutputTags(directInsert, TupleTagList.of(modifyTable).and(invalidCategory))
        );

        modifyOrInsert.get(directInsert).apply(
                   "DirectInsert/ConvertToTableRow",
                ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            Gson gson = new Gson();
                            String message = c.element();
                            TableRow row = gson.fromJson(message, TableRow.class);
                            System.out.println("data type: " + row.getClass() + "\ndata: " + row.toString());
                            c.output(row);
                        } catch (Exception e) {
                            System.out.println("Exception in Convert to BQRow step: " + e);
                            e.printStackTrace();
                        }
                    }
                })
        )/*.apply(
                "DirectInsertToBQ",
                BigQueryIO.writeTableRows()
                        .to((ValueInSingleWindow<TableRow> event) -> {
                            String tableDest;
                            tableDest = event.getValue().get("table_name").toString();
                            System.out.println("direct insert table_dest: " + tableDest);

                            // why the  hell is projectId null?
                            // TableReference tableRef = new TableReference(activeProjectId, activeDatasetId, tableDest);
                            TableDestination fullTableDest = new TableDestination(
                                    String.format("%s.%s.%s", activeProjectId, activeDatasetId, tableDest),
                                    "nulll"
                            );
                            LOG.info("direct insert fullTableDest: " + fullTableDest); // remove this later, for debugging
                            LOG.info("direct insert fullTableSpec: " + fullTableDest.getTableSpec());
                            return fullTableDest;
                        })
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
                        //.withoutValidation()
                        //.withTriggeringFrequency(Duration.standardDays(1))
        ).getFailedInsertsWithErr().apply("DirectInsert-GetFailedInserts", ParDo.of(new DoFn<*//*TableRow*//*BigQueryInsertError, Void>() {
            @ProcessElement
            public void processElement(final ProcessContext processContext) throws IOException {
                // System.out.println("Table Row : " + processContext.element().toPrettyString());
                // System.out.println("Table Row : " + processContext.element().toString());
                System.out.println("Table Row Error: " + processContext.element().getError());
                System.out.println("Table Row Error: " + processContext.element().getRow());
            }
        }));*/


        .apply(
                "DirectInsertToBQ",
                BigQueryIO.writeTableRows()
                        .to((ValueInSingleWindow<TableRow> event) -> {
                            String tableDest;
                            tableDest = event.getValue().get("table_name").toString();
                            System.out.println("direct insert table_dest: " + tableDest);

                            // why the  hell is projectId null?
                            // TableReference tableRef = new TableReference(activeProjectId, activeDatasetId, tableDest);
                            TableDestination fullTableDest = new TableDestination(
                                    String.format("%s.%s.%s", activeProjectId, activeDatasetId, tableDest),
                                    "nulll"
                            );
                            LOG.info("direct insert fullTableDest: " + fullTableDest); // remove this later, for debugging
                            LOG.info("direct insert fullTableSpec: " + fullTableDest.getTableSpec());
                            return fullTableDest;
                        })
                        //.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withNumStorageWriteApiStreams(5)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
        ).getFailedStorageApiInserts().apply("DirectInsert-GetFailedInserts", ParDo.of(new DoFn</*TableRow*/BigQueryStorageApiInsertError, Void>() {
            @ProcessElement
            public void processElement(final ProcessContext processContext) throws IOException {
                System.out.println("Table Row : " + processContext.element().toString());
                System.out.println("Table Row Error: " + processContext.element().getRow());

                LOG.error("(DirectInsert Insert) BQ insert error: " + processContext.element().getErrorMessage());
                LOG.error("(DirectInsert Insert) Invalid data: " + processContext.element().getRow());
            }
        }));



        modifyOrInsert.get(modifyTable).apply(
                "ModifyTable Insert/Convert to Table Row",
                ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            Gson gson = new Gson();
                            String message = c.element();
                            TableRow row = gson.fromJson(message, TableRow.class);
                            System.out.println("data type: " + row.getClass() + "\ndata: " + row);
                            c.output(row);
                        } catch (Exception e) {
                            System.out.println("Exception in Convert to BQRow step: " + e);
                            e.printStackTrace();
                        }
                    }
                })
        ).apply(
                "ModifyTable Insert to BQ",
                BigQueryIO.writeTableRows()
                        .to((ValueInSingleWindow<TableRow> event) -> {
                            String tableDest;
                            tableDest = event.getValue().get("table_name").toString();

                            LOG.debug("ModifyTable insert table_dest: " + tableDest);

                            TableDestination fullTableDest = new TableDestination(
                                    String.format("%s.%s.%s", activeProjectId, activeDatasetId, tableDest),
                                    "null"
                            );
                            return fullTableDest;
                        })
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
        ).getFailedInsertsWithErr().apply(
            "ModifyTable Get Failed Inserts",
            ParDo.of(new DoFn</*TableRow*/BigQueryInsertError, /*Void*/PubsubMessage>() {
            @ProcessElement
            public void processElement(final ProcessContext c) throws IOException {

                // make it a function
                LOG.error("(ModifyTable Insert) BQ insert error: " + c.element().getError());
                LOG.error("(ModifyTable Insert) Invalid data: " + c.element().getRow());

                HashMap<String, Object> invalidDataToPubSubMap = new HashMap<>();
                invalidDataToPubSubMap.put("error", c.element().getError());
                invalidDataToPubSubMap.put("data", c.element().getRow());
                invalidDataToPubSubMap.put("table", c.element().getTable());

                // convert to valid JSON, useful for downstream analysis
                String invalidData = new Gson().toJson(invalidDataToPubSubMap);

                // convert to PubSubMessage
                PubsubMessage pubsubMessage = new PubsubMessage(invalidData.getBytes(), null);
                c.output(pubsubMessage);
            }
        })).apply(
                "Invalid data to Pubsub",
                PubsubIO.writeMessages().to("projects/"+activeProjectId+"/topics/"+invalidPubsubTopic) // "projects/"+projectId+"/topics/"+invalidPubsubTopic
        );


        p.run().waitUntilFinish();
    }

    static class HelperFunctions {
        static private Map<String, Object> convertPubsubtoMap(String message) {
            // byte[] byteStringMessage = message.getPayload();
            // String jsonStringPayload = new String(byteStringMessage);

            JsonObject jsonObject = JsonParser.parseString(message).getAsJsonObject();
            HashMap eventMap = new Gson().fromJson(jsonObject, HashMap.class);

            return eventMap;
        }

        static private JsonObject convertPubsubtoJsonObject(String message) {
            // byte[] byteStringMessage = message.getPayload();
            // String jsonStringPayload = new String(byteStringMessage);

            JsonObject jsonObject = JsonParser.parseString(message).getAsJsonObject();
            return jsonObject;
        }

        static private boolean doesTableExist(String projectId, String datasetId, String tableName) {
            boolean doesExist = false;
            try {
                BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
                Table table = bigquery.getTable(TableId.of(datasetId, tableName));

                doesExist = table != null && table.exists();
            } catch (Exception e) {
                System.out.println("Ran into exception when checking if table exists: \n" + e);
            }

            return doesExist;
        }

        static private boolean doesTableExistInCache(TableId tableId) {
            return cache.containsKey(tableId);
        }

        static private String extractTableName(String message) { // input is String
            String tableName = "";

            if (!tableName.isEmpty() && tableName != null) {
                return tableName;
            } else {
                tableName = (String) HelperFunctions.convertPubsubtoMap(message).get("table_name");
            }
            // return (String) message.getAttributeMap().get("eventName");
            return tableName;
        }

        static private String extractTableName(Map<String, Object> elementMap) { // input becomes Hashmap
            // String tableName = (String) elementMap.get("table_name");
            return (String) elementMap.get("table_name");
        }

        // static private Boolean schemaMatches(String projectId, String datasetId, String tableName) {
        //     return true;
        // }

        static private JsonObject addTableName(JsonObject jsonObject) {

            JsonObject jsonData = jsonObject.get("data").getAsJsonObject();
            String tableName = jsonObject.get("table_name").getAsString();
            jsonData.addProperty("table_name", tableName);

            return jsonData;
        }

        static private Schema getTableSchema(Table table) {
            return table.getDefinition().getSchema();
        }

        static  private  HashMap<String, Object> createHashmapFromJsonObject (JsonObject jsonObject) {
            return new Gson().fromJson(jsonObject, HashMap.class);
        }

        // cant use because it parses everything as a string. getClass() on each value misleading
//        static private Schema createSchemaFromJsonEvent(JsonObject messageData) throws ParseException {
//            List<String> keyList = new ArrayList<>(messageData.keySet());
//            List<Field> fieldList = new ArrayList<>();
//
//            System.out.println(keyList.toString());
//            for (String fieldName : keyList) {
//                // String fieldName = key;
//                Object value = messageData.get(fieldName);
//                System.out.println(String.format("Field name: %s Field value: %s field type: %s", fieldName, value.toString(), value.getClass()));
//                if (fieldName.contains("time")) {
//                    DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
//                    Date date = formatter.parse((String) value);
//
//                    Timestamp timeStampDate = new Timestamp(date.getTime());
//                     System.out.println("Field name: " + fieldName + ", Detected Field type: " + (timeStampDate).getClass());
//                    StandardSQLTypeName fieldType = com.google.cloud.bigquery.StandardSQLTypeName.TIMESTAMP; //getFieldType(value);
//
//                    Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.NULLABLE).build();
//                    fieldList.add(field);
//                    continue;
//                }
//
//                if (value.getClass() == ArrayList.class) {
//                     System.out.println("Field name: " + fieldName + ", Detected Field type: " + (value).getClass());
//                    StandardSQLTypeName fieldType = typeMapping.get(((ArrayList<?>) value).get(0).getClass());
//                     System.out.println("Mapped Field type: " + fieldType.toString());
//                    Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.REPEATED).build();
//                    fieldList.add(field);
//                    continue;
//                }
//
//                // System.out.println("Field name: " + fieldName + ", Detected Field type: " + value.getClass());
//                // StandardSQLTypeName fieldType = com.google.cloud.bigquery.StandardSQLTypeName.STRING; //getFieldType(value);
//                StandardSQLTypeName fieldType = typeMapping.get(value.getClass());
//                Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.NULLABLE).build();
//                fieldList.add(field);
//            }
//
//            Schema schema = Schema.of(fieldList);
//
//            return schema;
//        }
        static private Schema createSchemaFromEvent(/*LinkedTreeMap*/ HashMap messageData) throws ParseException {
            List<String> keyList = new ArrayList<>(messageData.keySet());
            List<Field> fieldList = new ArrayList<>();

            for (String key : keyList) {
                String fieldName = key;
                Object value = messageData.get(fieldName);
                if (fieldName.contains("time")) {
                    DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
                    Date date = formatter.parse((String) value);

                    // System.out.println("Field name: " + fieldName + ", Detected Field type: " + (timeStampDate).getClass());
                    StandardSQLTypeName fieldType = com.google.cloud.bigquery.StandardSQLTypeName.TIMESTAMP; //getFieldType(value);

                    Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.NULLABLE).build();
                    fieldList.add(field);
                    continue;
                }

                if (value.getClass() == ArrayList.class) {
                    // System.out.println("Field name: " + fieldName + ", Detected Field type: " + (value).getClass());
                    StandardSQLTypeName fieldType = typeMapping.get(((ArrayList<?>) value).get(0).getClass());
                    // System.out.println("Mapped Field type: " + fieldType.toString());
                    Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.REPEATED).build();
                    fieldList.add(field);
                    continue;
                }

                // System.out.println("Field name: " + fieldName + ", Detected Field type: " + value.getClass());
                // StandardSQLTypeName fieldType = com.google.cloud.bigquery.StandardSQLTypeName.STRING; //getFieldType(value);
                StandardSQLTypeName fieldType = typeMapping.get(value.getClass());
                Field field = Field.newBuilder(fieldName, fieldType).setMode(Field.Mode.NULLABLE).build();
                fieldList.add(field);
            }


            Schema schema = Schema.of(fieldList);

            return schema;
        }
    }

    // Caching (LRU)
    public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        public LRUCache(int capacity) {
            super(capacity, 0.75f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > this.capacity;
        }
    }

//        static class Branch extends /*DoFn<PubsubMessage, KV<PubsubMessage, String>>*/  DoFn<String, String> {
//
//            @ProcessElement
//            public void processElement(DoFn.ProcessContext context) {
//
//                String message = (String) context.element();
//                String tableName = HelperFunctions.extractTableName(message);
//
//                if (!HelperFunctions.doesTableExist(projectId, datasetId, tableName)) {
//                    context.output(createTable, message);
//                }
//
//                if (HelperFunctions.doesTableExist(projectId, datasetId, tableName)) {
//                    Map<String, String> out = new HashMap<>();
//
//                    if (true) {
//                        context.output(insertTable, message);
//                    } else {
//                        context.output(modifyTable, message);
//                    }
//                }
//
//            }
//        }


}

/*
* ASSUMPTIONS
* All ingestion is to the same dataset in Bigquery
* All ingestion is to the same project on GCP
* */

/*
* Other General ideas
* 1. Use caching to store schema once generated, verify schema of table with schema in cache
*       use special cache with TTL or cache hit that keeps only frequently used table
*           eg. https://crunchify.com/how-to-create-a-simple-in-memory-cache-in-java-lightweight-cache/
* 2. make code more DRY
*   - can I do something with all the helper functions?
*       Do they need their own class? or just be methods in the DynamicDataflowPipeline Class?
*   - Consider using flatten to merge the output of ModifyTableConvertToTableRow and DirectInsertConvertToTableRow - https://beam.apache.org/documentation/transforms/java/other/flatten/
*       because, I should be able to use the same BigQueryWriteIO DoFn and errorHandling after merging
* 3. Duplicates at the beginning, when there is a flurry of events.... FIX IT
* */

/*
* ISSUES
* 1. Running into Exceeded Rate limit
*   Possible cause:
*       When checking if table exists, it makes an api call and there is api limiting
*       Currently (Apr 6,2024), the workflow is to check that the table exists for each event, not sustainable I guess, if it does not exist, create the table but if it does, insert or modify table
*       SOLUTION IDEA: (DONE-ISH, need to test the limits of the cache, e.g. what happens when it is full or at capacity)
*       https://www.linkedin.com/pulse/building-lru-cache-java-simple-approach-using-weberton-faria-0cglc#:~:text=The%20LRU%20Cache%20can%20be,abstracted%20using%20the%20LinkedHashMap%20class.
*           Implement caching: when a table is created, it stores the table definition in a map like object (cache), instead of checking BigQuery itself, we check the cache, then bigquery
*           if the table is not found then create the table and store in the cache.
*
* 2. too much logging...smh
*   SOLUTION IDEA:
*       Remove logging (any level lower than WARNING - INFO, DEBUG, DEFAULT and friends) than for direct insert, most of the workload is there anyway.
*       or change to debug, the minimum level seems to be info anyway 🤷‍♂️
* */