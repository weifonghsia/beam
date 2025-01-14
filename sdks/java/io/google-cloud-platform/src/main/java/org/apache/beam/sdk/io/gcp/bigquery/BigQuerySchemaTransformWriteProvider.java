/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery write jobs configured
 * using {@link BigQuerySchemaTransformWriteConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
@Internal
@Experimental(Kind.SCHEMAS)
public class BigQuerySchemaTransformWriteProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaTransformWriteConfiguration> {

  private static final String API = "bigquery";
  static final String INPUT_TAG = "INPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<BigQuerySchemaTransformWriteConfiguration> configurationClass() {
    return BigQuerySchemaTransformWriteConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(BigQuerySchemaTransformWriteConfiguration configuration) {
    return new BigQueryWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:write", API);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since a
   * single is expected, this returns a list with a single name.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * no output is expected, this returns an empty list.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * A {@link SchemaTransform} that performs {@link BigQueryIO.Write}s based on a {@link
   * BigQuerySchemaTransformWriteConfiguration}.
   */
  private static class BigQueryWriteSchemaTransform implements SchemaTransform {
    private final BigQuerySchemaTransformWriteConfiguration configuration;

    BigQueryWriteSchemaTransform(BigQuerySchemaTransformWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    /**
     * Overrides {@link SchemaTransform#buildTransform()} by returning a {@link
     * PCollectionRowTupleTransform}.
     */
    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PCollectionRowTupleTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for BigQuery write jobs configured using {@link
   * BigQuerySchemaTransformWriteConfiguration}.
   */
  static class PCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {

    private final BigQuerySchemaTransformWriteConfiguration configuration;

    /** An instance of {@link BigQueryServices} used for testing. */
    private BigQueryServices testBigQueryServices = null;

    PCollectionRowTupleTransform(BigQuerySchemaTransformWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public void validate(PipelineOptions options) {
      if (!configuration.getCreateDisposition().equals(CreateDisposition.CREATE_NEVER.name())) {
        return;
      }

      BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);

      BigQueryServices bigQueryServices = new BigQueryServicesImpl();
      if (testBigQueryServices != null) {
        bigQueryServices = testBigQueryServices;
      }

      DatasetService datasetService = bigQueryServices.getDatasetService(bigQueryOptions);
      TableReference tableReference = BigQueryUtils.toTableReference(configuration.getTableSpec());

      try {
        Table table = datasetService.getTable(tableReference);
        if (table == null) {
          throw new NullPointerException();
        }

        if (table.getSchema() == null) {
          throw new InvalidConfigurationException(
              String.format("could not fetch schema for table: %s", configuration.getTableSpec()));
        }

      } catch (NullPointerException | InterruptedException | IOException ex) {
        throw new InvalidConfigurationException(
            String.format(
                "could not fetch table %s, error: %s",
                configuration.getTableSpec(), ex.getMessage()));
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      validate(input);
      PCollection<Row> rowPCollection = input.get(INPUT_TAG);
      Schema schema = rowPCollection.getSchema();
      BigQueryIO.Write<TableRow> write = toWrite(schema);
      if (testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      PCollection<TableRow> tableRowPCollection =
          rowPCollection.apply(
              MapElements.into(TypeDescriptor.of(TableRow.class)).via(BigQueryUtils::toTableRow));
      tableRowPCollection.apply(write);
      return PCollectionRowTuple.empty(input.getPipeline());
    }

    /** Instantiates a {@link BigQueryIO.Write<TableRow>} from a {@link Schema}. */
    BigQueryIO.Write<TableRow> toWrite(Schema schema) {
      TableSchema tableSchema = BigQueryUtils.toTableSchema(schema);
      CreateDisposition createDisposition =
          CreateDisposition.valueOf(configuration.getCreateDisposition());
      WriteDisposition writeDisposition =
          WriteDisposition.valueOf(configuration.getWriteDisposition());

      return BigQueryIO.writeTableRows()
          .to(configuration.getTableSpec())
          .withCreateDisposition(createDisposition)
          .withWriteDisposition(writeDisposition)
          .withSchema(tableSchema);
    }

    /** Setter for testing using {@link BigQueryServices}. */
    @VisibleForTesting
    void setTestBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    /** Validate a {@link PCollectionRowTuple} input. */
    void validate(PCollectionRowTuple input) {
      if (!input.has(INPUT_TAG)) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s is missing expected tag: %s",
                getClass().getSimpleName(), input.getClass().getSimpleName(), INPUT_TAG));
      }

      PCollection<Row> rowInput = input.get(INPUT_TAG);
      Schema sourceSchema = rowInput.getSchema();

      if (sourceSchema == null) {
        throw new IllegalArgumentException(
            String.format("%s is null for input of tag: %s", Schema.class, INPUT_TAG));
      }

      if (!configuration.getCreateDisposition().equals(CreateDisposition.CREATE_NEVER.name())) {
        return;
      }

      BigQueryOptions bigQueryOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);

      BigQueryServices bigQueryServices = new BigQueryServicesImpl();
      if (testBigQueryServices != null) {
        bigQueryServices = testBigQueryServices;
      }

      DatasetService datasetService = bigQueryServices.getDatasetService(bigQueryOptions);
      TableReference tableReference = BigQueryUtils.toTableReference(configuration.getTableSpec());

      try {
        Table table = datasetService.getTable(tableReference);
        if (table == null) {
          throw new NullPointerException();
        }

        TableSchema tableSchema = table.getSchema();
        if (tableSchema == null) {
          throw new NullPointerException();
        }

        Schema destinationSchema = BigQueryUtils.fromTableSchema(tableSchema);
        if (destinationSchema == null) {
          throw new NullPointerException();
        }

        validateMatching(sourceSchema, destinationSchema);

      } catch (NullPointerException | InterruptedException | IOException e) {
        throw new InvalidConfigurationException(
            String.format(
                "could not validate input for create disposition: %s and table: %s, error: %s",
                configuration.getCreateDisposition(),
                configuration.getTableSpec(),
                e.getMessage()));
      }
    }

    void validateMatching(Schema sourceSchema, Schema destinationSchema) {
      if (!sourceSchema.equals(destinationSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "source and destination schema mismatch for table: %s",
                configuration.getTableSpec()));
      }
    }
  }
}
