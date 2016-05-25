package databaseconnectortest.tools;

import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DeleteClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.InvalidClusterStateException;
import com.mendix.logging.ILogNode;

import databaseconnectortest.proxies.constants.Constants;

public class RedshiftClusterOperations {

  private AmazonRedshiftClient client;
  private String clusterIdentifier = Constants.getClusterIdentifier();
  private long sleepTime = 20;
  private String clusterStatusAvailable = "available";
  private String clusterStatusCreating = "creating";
  private String clusterStatusDeleting = "deleting";
  private ILogNode logNode;

  public RedshiftClusterOperations(ILogNode logNode) {
    this.logNode = logNode;
    createRedShiftClient();
  }

  private void createRedShiftClient() {
    Region region = Region.getRegion(Regions.fromName(Constants.getClusterRegion()));
    BasicAWSCredentials credentials = new BasicAWSCredentials(Constants.getAwsAccessKey(), Constants.getAwsSecretKey());
    client = new AmazonRedshiftClient(credentials);
    client.setRegion(region);
  }

  public void createCluster() {
    if (hasStatus(clusterStatusDeleting)) {
      Supplier<Boolean> isDeleting = () -> hasStatus(clusterStatusDeleting);
      OperationBlocker.blockUntilReady(isDeleting, sleepTime, logNode, clusterStatusDeleting);
    }

    if (noClusterExists()) {
      CreateClusterRequest request = new CreateClusterRequest()
          .withClusterIdentifier(clusterIdentifier)
          .withMasterUsername(Constants.getClusterUserName())
          .withMasterUserPassword(Constants.getClusterPassword())
          .withNodeType(Constants.getClusterNodeType())
          .withClusterType(Constants.getClusterType())
          .withVpcSecurityGroupIds(Constants.getVpcSecurityGroupId())
          .withAvailabilityZone(Constants.getClusterAvailabilityZone());

      Cluster createResponse = client.createCluster(request);

      logNode.info("Cluster with cluster identifier " + createResponse.getClusterIdentifier() + " created successfully!");
    } else {
      logNode.info("Cluster " + clusterIdentifier + " will not be created because it already exists!");
    }

    if (hasStatus(clusterStatusCreating)) {
      Supplier<Boolean> isCreating = () -> hasStatus(clusterStatusCreating);
      OperationBlocker.blockUntilReady(isCreating, sleepTime, logNode, clusterStatusCreating);
    }
  }

  public String getJdbcUrl() {
    DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest()
        .withClusterIdentifier(clusterIdentifier);
    Cluster cluster = client.describeClusters(describeClustersRequest).getClusters().get(0);
    return String.format("jdbc:redshift://%s:%s/dev",
        cluster.getEndpoint().getAddress(), cluster.getEndpoint().getPort());
  }

  public void deleteCluster() {
    if (!hasStatus(clusterStatusAvailable)) {
      logNode.info("Cluster " + clusterIdentifier + " doesn't exist or is being operated currently! Try deleting at a later time.");
      return;
    }

    DeleteClusterRequest deleteClusterRequest = new DeleteClusterRequest()
        .withClusterIdentifier(clusterIdentifier)
        .withSkipFinalClusterSnapshot(true);

    try {
      client.deleteCluster(deleteClusterRequest);
    } catch (InvalidClusterStateException ics) {
      logNode.info("An operation is running on the Cluster. Please try to delete it at a later time.");
    }
    logNode.info(clusterIdentifier + " cluster will be deleted!");
  }

  private boolean noClusterExists() {
    Predicate<Cluster> predicate = c -> c.getClusterIdentifier().equals(clusterIdentifier);
    boolean noClusterExists = client.describeClusters().getClusters().stream().noneMatch(predicate);
    return noClusterExists;
  }

  private boolean hasStatus(String status) {
    Stream<Cluster> clustersStream = client.describeClusters().getClusters().stream();
    Predicate<Cluster> predicate = c -> c.getClusterIdentifier().equals(clusterIdentifier) &&
        c.getClusterStatus().equals(status);

    return clustersStream.anyMatch(predicate);
  }
}
