package com.amazonaws.glue.catalog.metastore;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.AbstractThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AlterDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.AlterDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableResponse;
import org.apache.hadoop.hive.metastore.api.AppendPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CreateDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.CreateDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDataConnectorRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionRequest;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PropertyGetRequest;
import org.apache.hadoop.hive.metastore.api.PropertyGetResponse;
import org.apache.hadoop.hive.metastore.api.PropertySetRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchResponse;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class AWSCatalogIface extends AbstractThriftHiveMetastore {
  private static final Logger logger = Logger.getLogger(AWSCatalogIface.class);

  private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;

  public AWSCatalogIface(Configuration conf) throws MetaException {
    final Warehouse wh = new Warehouse(conf);
    final AWSGlueMetastore glueMetastore = new AWSGlueMetastoreFactory().newMetastore(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(conf, glueMetastore, wh);

    final AWSGlue glueClient = new AWSGlueClientFactory(conf).newClient();
    final String catalogId = MetastoreClientUtils.getCatalogId(conf);
    if (!doesDefaultDBExist(glueClient, catalogId)) {
      createDefaultDatabase(wh);
    }
  }

  private boolean doesDefaultDBExist(AWSGlue glueClient, String catalogId) throws MetaException {
    try {
      com.amazonaws.services.glue.model.GetDatabaseRequest getDatabaseRequest =
          new com.amazonaws.services.glue.model.GetDatabaseRequest()
              .withName(DEFAULT_DATABASE_NAME)
              .withCatalogId(catalogId);
      glueClient.getDatabase(getDatabaseRequest);
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AmazonServiceException e) {
      String msg = "Unable to verify existence of default database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
    return true;
  }

  private void createDefaultDatabase(Warehouse wh) throws MetaException {
    Database defaultDB = new Database();
    defaultDB.setName(DEFAULT_DATABASE_NAME);
    defaultDB.setDescription(DEFAULT_DATABASE_COMMENT);
    defaultDB.setLocationUri(wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString());

    org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet principalPrivilegeSet
        = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
    principalPrivilegeSet.setRolePrivileges(Maps.<String, List<org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo>>newHashMap());

    defaultDB.setPrivileges(principalPrivilegeSet);

    /**
     * TODO: Grant access to role PUBLIC after role support is added
     */
    try {
      create_database(defaultDB);
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      logger.warn("database - default already exists. Ignoring..");
    } catch (Exception e) {
      logger.error("Unable to create default database", e);
    }
  }

  @Override
  public void create_database(Database database) throws TException {
    glueMetastoreClientDelegate.createDatabase(database);
  }

  @Override
  public Database get_database_req(GetDatabaseRequest request) throws TException {
    return glueMetastoreClientDelegate.getDatabase(request.getName());
  }

  @Override
  public void create_table_req(CreateTableRequest request) throws TException {
    glueMetastoreClientDelegate.createTable(request.getTable());
  }

  @Override
  public AlterTableResponse alter_table_req(AlterTableRequest req) throws TException {
    glueMetastoreClientDelegate.alterTable(req.getDbName(), req.getTableName(), req.getTable(),
        req.getEnvironmentContext());
    return new AlterTableResponse();
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws TException {
    final GetTableResult result = new GetTableResult();
    result.setTable(glueMetastoreClientDelegate.getTable(req.getDbName(), req.getTblName()));
    return result;
  }

  @Override
  public GetAllFunctionsResponse get_all_functions() throws TException {
    return glueMetastoreClientDelegate.getAllFunctions();
  }

  @Override
  public WMGetActiveResourcePlanResponse get_active_resource_plan(
      WMGetActiveResourcePlanRequest request) {
    return new WMGetActiveResourcePlanResponse();
  }

  @Override
  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request) {
    return new NotNullConstraintsResponse();
  }

  @Override
  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request) {
    return new CheckConstraintsResponse();
  }

  @Override
  public void flushCache() {
  }

  @Override
  public void shutdown() {
  }





  // AbstractThriftHiveMetastore should have the default implementation
  @Override
  public AbortCompactResponse abort_Compactions(AbortCompactionRequest abortCompactionRequest) {
    throw new AssertionError("Not implemented");
  }

  @Override
  public void create_database_req(CreateDatabaseRequest createDatabaseRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void drop_database_req(DropDatabaseRequest dropDatabaseRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void alter_database_req(AlterDatabaseRequest alterDatabaseRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void create_dataconnector_req(CreateDataConnectorRequest createDataConnectorRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void drop_dataconnector_req(DropDataConnectorRequest dropDataConnectorRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void alter_dataconnector_req(AlterDataConnectorRequest alterDataConnectorRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Table translate_table_dryrun(CreateTableRequest createTableRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void drop_table_req(DropTableRequest dropTableRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Partition append_partition_req(AppendPartitionsRequest appendPartitionsRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean drop_partition_req(DropPartitionRequest dropPartitionRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<String> fetch_partition_names_req(PartitionsRequest partitionsRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PropertyGetResponse get_properties(PropertyGetRequest propertyGetRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean set_properties(PropertySetRequest propertySetRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void update_transaction_statistics(
      UpdateTransactionalStatsRequest updateTransactionalStatsRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void add_write_ids_to_min_history(long l, Map<String, Long> map) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean submit_for_cleanup(CompactionRequest compactionRequest, long l, long l1) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void mark_refused(CompactionInfoStruct compactionInfoStruct) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean update_compaction_metrics_data(
      CompactionMetricsDataStruct compactionMetricsDataStruct) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void remove_compaction_metrics_data(
      CompactionMetricsDataRequest compactionMetricsDataRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public WriteNotificationLogBatchResponse add_write_notification_log_in_batch(
      WriteNotificationLogBatchRequest writeNotificationLogBatchRequest) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
