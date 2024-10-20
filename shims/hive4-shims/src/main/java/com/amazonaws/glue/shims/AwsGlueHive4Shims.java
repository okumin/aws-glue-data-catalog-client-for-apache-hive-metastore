package com.amazonaws.glue.shims;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

final class AwsGlueHive4Shims implements AwsGlueHiveShims {

  private static final String HIVE_4_VERSION = "4.";

  static boolean supportsVersion(String version) {
    return version.startsWith(HIVE_4_VERSION);
  }

  @Override
  public ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes) {
    return SerializationUtilities
        .deserializeObjectFromKryo(exprBytes, ExprNodeGenericFuncDesc.class);
  }

  @Override
  public byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr) {
    return SerializationUtilities.serializeObjectToKryo(expr);
  }

  @Override
  public Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse) throws MetaException {
    return warehouse.getDefaultTablePath(db, tableName, false);
  }

  @Override
  public boolean deleteDir(Warehouse wh, Path path, boolean recursive, boolean ifPurge) throws MetaException {
    return wh.deleteDir(path, recursive, ifPurge, true);
  }

  @Override
  public boolean mkdirs(Warehouse wh, Path path) throws MetaException {
    return wh.mkdirs(path);
  }

  @Override
  public boolean validateTableName(String name, Configuration conf) {
    return MetaStoreUtils.validateName(name, conf);
  }

  @Override
  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    return MetaStoreServerUtils.requireCalStats(oldPart, newPart, tbl, environmentContext);
  }

  @Override
  public boolean updateTableStatsFast(
      Database db,
      Table tbl,
      Warehouse wh,
      boolean madeDir,
      boolean forceRecompute,
      EnvironmentContext environmentContext
  ) throws MetaException {
    MetaStoreServerUtils.updateTableStatsSlow(db, tbl, wh, madeDir, forceRecompute, environmentContext);
    return true;
  }

  @Override
  public String validateTblColumns(List<FieldSchema> cols) {
    return MetaStoreServerUtils.validateTblColumns(cols);
  }

}
