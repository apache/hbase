package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.RefreshHFilesCallable;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

@InterfaceAudience.Private
public class RefreshHFilesRegionProcedure extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface,
  RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv, ServerName> {
  private RegionInfo region;

  public RefreshHFilesRegionProcedure() {
  }

  public RefreshHFilesRegionProcedure(RegionInfo region) {
    this.region = region;
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.RefreshHFilesRegionProcedureStateData data =
      serializer.deserialize(MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.class);
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
    // TODO Get the Data from region server
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.Builder builder = MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    // TODO add data that you want to pass to region server
    serializer.serialize(builder.build());
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionStateNode regionNode = regionStates.getRegionStateNode(region);

    ServerName targetServer = regionNode.getRegionLocation();

    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
    } catch (FailedRemoteDispatchException e) {
      throw new ProcedureSuspendedException();
    }


    return null;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REFRESH_HFILES;
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    // TODO redo the same thing again till retry count else send the error to client.
  }

  public void remoteOperationCompleted(MasterProcedureEnv env) {
    // TODO Do nothing just LOG completed successfully as everything is completed successfully
  }


  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName serverName, IOException e) {
    // TODO redo the same thing again till retry count else send the error to client.
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    MasterProcedureProtos.RefreshHFilesRegionParameter.Builder builder = MasterProcedureProtos.RefreshHFilesRegionParameter.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    // TODO Add logic on how to build remote call
    return Optional
      .of(new RSProcedureDispatcher.ServerOperation(this, getProcId(), RefreshHFilesCallable.class,
        builder.build().toByteArray(), env.getMasterServices().getMasterActiveTime()));
  }
}
