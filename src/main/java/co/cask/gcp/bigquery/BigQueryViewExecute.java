/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.hydrator.common.Constants;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class <code>BigQueryExecute</code> executes a single Cloud BigQuery SQL.
 * <p>
 * The plugin provides the ability different options like choosing interactive or batch execution of sql query, setting
 * of resulting dataset and table, enabling/disabling cache, specifying whether the query being executed is legacy or
 * standard and retry strategy.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryViewExecute.NAME)
@Description("Execute a Google BigQuery SQL predicate against existing view.")
public final class BigQueryViewExecute extends Action {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryViewExecute.class);
    public static final String NAME = "BigQueryViewExecute";
    private static final String RECORDS_OUT = "records.out";
    private static final String MODE_BATCH = "batch";
    private static final String DS_NAME = "MY_DATASET_1";

    private Config config;

    @Override
    public void run(ActionContext context) throws Exception {
        config.validate();

        Schema sourceSchema = getSchema(
                config.getDataset(), "cdap_view_1", config.getProject(), config.getServiceAccountFilePath());

        String querySql = "SELECT * FROM " + config.getView() + " WHERE " + config.getSqlPredicate();

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(querySql);

        builder.setPriority(config.getMode().equalsIgnoreCase(MODE_BATCH)
                ? QueryJobConfiguration.Priority.BATCH
                : QueryJobConfiguration.Priority.INTERACTIVE
        );

        // Save the results of the query to a permanent table.
        if (config.getDataset() != null && config.getTable() != null) {
            builder.setDestinationTable(TableId.of(config.getDataset(), config.getTable()));
            builder.setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER);
            builder.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND);
            context.getArguments().set(context.getStageName() + ".dataset", config.getDataset());
            context.getArguments().set(context.getStageName() + ".table", config.getTable());
        }

        // Enable or Disable the query cache to force live query evaluation.
        if (config.shouldUseCache()) {
            builder.setUseQueryCache(true);
        }

        // Enable legacy SQL
        builder.setUseLegacySql(config.isLegacySQL());

        QueryJobConfiguration queryConfig = builder.build();

        // Location must match that of the dataset(s) referenced in the query.
        JobId jobId = JobId.newBuilder().setRandomJob().setLocation(config.getLocation()).build();

        LineageRecorder lineageRecorder = new LineageRecorder(context, DS_NAME);

        // API request - starts the query.
        BigQuery bigQuery = BigQueryUtils.getBigQuery(config.getServiceAccountFilePath(), config.getProject());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        LOG.info("Executing query '{}'. The Google BigQuery job id is '{}'.", querySql, jobId.getJob());

        // Wait for the query to complete
        RetryOption retryOption = RetryOption.totalTimeout(Duration.ofMinutes(config.getQueryTimeoutInMins()));
        queryJob.waitFor(retryOption);

        emitLineage(lineageRecorder, sourceSchema, context);
        lineageRecorder.releaseDs();

        // Check for errors
        if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getExecutionErrors().toString());
        }

        TableResult queryResults = queryJob.getQueryResults();
        long rows = queryResults.getTotalRows();

        context.getMetrics().gauge(RECORDS_OUT, rows);
        context.getArguments().set(context.getStageName().concat(".query"), querySql);
        context.getArguments().set(context.getStageName().concat(".jobid"), jobId.getJob());
        context.getArguments().set(context.getStageName().concat(RECORDS_OUT), String.valueOf(rows));
    }

    @Override
    public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
        super.configurePipeline(configurer);

        config.validate();

//        configurer.createDataset(DS_NAME, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    }

    private void emitLineage(LineageRecorder lineageRecorder, Schema schema, ActionContext context) {
        lineageRecorder.createExternalDataset(schema);

        if (schema.getFields() != null) {
            lineageRecorder.recordRead("Read", "Read from BigQuery table.",
                    schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
        }
    }

    private Schema getSchema(
            String dataset, String tableName, String project, String serviceAccountFilePath
    ) throws Exception {
        Table table = BigQueryUtils.getBigQueryTable(serviceAccountFilePath, project, dataset, tableName);
        if (table == null) {
            // Table does not exist
            throw new IllegalArgumentException(String.format("BigQuery table '%s:%s.%s' does not exist",
                    project, dataset, tableName));
        }
        com.google.cloud.bigquery.Schema bgSchema = table.getDefinition().getSchema();
        if (bgSchema == null) {
            throw new IllegalArgumentException(
                    String.format("Cannot read from table '%s:%s.%s' because it has no schema.",
                            project, dataset, table));
        }
        List<Schema.Field> fields = getSchemaFields(bgSchema);
        return Schema.recordOf("output", fields);
    }

    private List<Schema.Field> getSchemaFields(com.google.cloud.bigquery.Schema bgSchema)
            throws UnsupportedTypeException {
        List<Schema.Field> fields = new ArrayList<>();
        for (Field field : bgSchema.getFields()) {
            LegacySQLTypeName type = field.getType();
            Schema schema;
            StandardSQLTypeName value = type.getStandardType();
            if (value == StandardSQLTypeName.FLOAT64) {
                // float is a float64, so corresponding type becomes double
                schema = Schema.of(Schema.Type.DOUBLE);
            } else if (value == StandardSQLTypeName.BOOL) {
                schema = Schema.of(Schema.Type.BOOLEAN);
            } else if (value == StandardSQLTypeName.INT64) {
                // int is a int64, so corresponding type becomes long
                schema = Schema.of(Schema.Type.LONG);
            } else if (value == StandardSQLTypeName.STRING) {
                schema = Schema.of(Schema.Type.STRING);
            } else if (value == StandardSQLTypeName.BYTES) {
                schema = Schema.of(Schema.Type.BYTES);
            } else if (value == StandardSQLTypeName.TIME) {
                schema = Schema.of(Schema.LogicalType.TIME_MICROS);
            } else if (value == StandardSQLTypeName.DATE) {
                schema = Schema.of(Schema.LogicalType.DATE);
            } else if (value == StandardSQLTypeName.TIMESTAMP || value == StandardSQLTypeName.DATETIME) {
                schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
            } else {
                // this should never happen
                throw new UnsupportedTypeException(String.format("BigQuery column '%s' is of unsupported type '%s'.",
                        field.getName(), value));
            }

            if (field.getMode() == null || field.getMode() == Field.Mode.NULLABLE) {
                fields.add(Schema.Field.of(field.getName(), Schema.nullableOf(schema)));
            } else if (field.getMode() == Field.Mode.REQUIRED) {
                fields.add(Schema.Field.of(field.getName(), schema));
            } else if (field.getMode() == Field.Mode.REPEATED) {
                throw new UnsupportedTypeException(
                        String.format("BigQuery column '%s' is of unsupported mode 'repeated'.", field.getName()));
            }
        }
        return fields;
    }

    /**
     * Config for the plugin.
     */
    public final class Config extends GCPReferenceSourceConfig {
        private static final long serialVersionUID = 465660676877986412L;
        @Description("Use Legacy SQL.")
        @Macro
        private String legacy;

        @Description("The view to apply the predicate to.")
        @Macro
        private String view;

        @Description("SQL query predicate to execute.")
        @Macro
        private String sqlPredicate;

        @Description("Mode to execute the query in. The value must be 'batch' or 'interactive'. " +
                "An interactive query is executed as soon as possible and " +
                "count towards the concurrent rate limit and the daily rate limit. " +
                "A batch query is queued and started as soon as idle resources are available, " +
                "usually within a few minutes. " +
                "If the query hasn't started within 3 hours, its priority is changed to 'INTERACTIVE'")
        @Macro
        private String mode;

        @Description("Use the cache when executing the query.")
        @Macro
        private String cache;

        @Description("Location of the job. Must match the location of the dataset specified in the query. " +
                "Defaults to 'US'")
        @Macro
        private String location;

        @Description("The dataset to store the query results in. If not specified, the results will not be stored.")
        @Macro
        @Nullable
        private String dataset;

        @Description("The table to store the query results in. If not specified, the results will not be stored.")
        @Macro
        @Nullable
        private String table;

        @Name("timeout")
        @Description("Query timeout in minutes. Defaults to 10.")
        @Macro
        private Long queryTimeoutInMins;

        public Long getQueryTimeoutInMins() {
            return queryTimeoutInMins == null ? 10 : queryTimeoutInMins;
        }

        public boolean isLegacySQL() {
            return legacy.equalsIgnoreCase("true");
        }

        public boolean shouldUseCache() {
            return cache.equalsIgnoreCase("true");
        }

        public String getLocation() {
            return location;
        }

        public String getLegacy() {
            return legacy;
        }

        public String getView() {
            return view;
        }

        public String getSqlPredicate() {
            return sqlPredicate;
        }

        public String getMode() {
            return mode;
        }

        public String getCache() {
            return cache;
        }

        @Nullable
        public String getDataset() {
            return dataset;
        }

        @Nullable
        public String getTable() {
            return table;
        }

        @Override
        public void validate() {
            if (!containsMacro("view") && (view == null || view.isEmpty())) {
                throw new IllegalArgumentException("View is not specified. Please specify a view to execute");
            }

            if (!containsMacro("sqlPredicate") && (sqlPredicate == null || sqlPredicate.isEmpty())) {
                throw new IllegalArgumentException("SQL predicate is not specified. " +
                        "Please specify a SQL predicate to execute");
            }

            // validates that either they are null together or not null together
            if ((dataset == null && table != null) || (table == null && dataset != null)) {
                throw new IllegalArgumentException("Dataset and table must be specified together.");
            }

            if (dataset != null) {
                // if one is not null then we know another is not null either. Now validate they are empty or non-empty
                // together
                if ((dataset.isEmpty() && !table.isEmpty()) ||
                        (table.isEmpty() && !dataset.isEmpty())) {
                    throw new IllegalArgumentException("Dataset and table must be specified together.");
                }
            }
        }
    }

    /**
     *
     */
    public final class LineageRecorder {
        private final CustomActionContext context;
        private final String dataset;
        private final Dataset ds;

        LineageRecorder(ActionContext context, String dataset) throws Exception {
            this.dataset = dataset;

            java.lang.reflect.Field f = context.getClass().getDeclaredField("context");
            f.setAccessible(true);
            this.context = (CustomActionContext) f.get(context);

            this.context.getAdmin().createDataset(
                    DS_NAME, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);

            ds = this.context.getDataset(DS_NAME);
        }

        void releaseDs() {
            this.context.releaseDataset(ds);
        }

        public void createExternalDataset(Schema schema) {
            DatasetProperties datasetProperties = DatasetProperties.of(
                    Collections.singletonMap(DatasetProperties.SCHEMA, schema.toString()));
            try {
                if (!context.getAdmin().datasetExists(dataset)) {
                    // if the dataset does not already exists then create it with the given schema.
                    // If it does exists then there is no need to create it.
                    context.getAdmin().createDataset(dataset, Constants.EXTERNAL_DATASET_TYPE, datasetProperties);
                }
            } catch (InstanceConflictException e) {
                // This will happen when multiple pipelines run simultaneously and are trying to create the same
                // external dataset.
                // Both might enter the if block after checking for existence and try to create the dataset.
                // One will succeed and another will receive a InstanceConflictException.
                // This exception can be ignored.
            } catch (DatasetManagementException e) {
                throw new RuntimeException(
                        String.format("Failed to create dataset %s with schema %s.", dataset, schema), e);
            }
        }

        private void recordRead(String operationName, String operationDescription, List<String> fields) {
//            context.record(Collections.singletonList(new FieldReadOperation(operationName,
//                    operationDescription,
//                    EndPoint.of(context.getNamespace(), dataset),
//                    fields)));
        }
    }
}
