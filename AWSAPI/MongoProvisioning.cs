using AWSAPI.Infrastructure.Virtualization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB;
using MongoDB.Driver;
using MongoDB.Bson;
namespace AWSAPI
{
    /// <summary>
    /// Support for provisioning MongoDB database instances on AWS
    /// </summary>
    public class AWSMongoManager:IDisposable
    {
        int configPort; //Config server
        int databasePort; // Database server
        int shardPort; //shard server (router)
        CloudController controller;
        Amazon.EC2.InstanceType size;
        List<string> securityGroups;
        CloudImage image;
        MongoClient configClient;
        MongoClient databaseClient;
        

        /// <summary>
        /// Creates a new management instance
        /// </summary>
        /// <param name="configServerPort">Port number for configuration servers</param>
        /// <param name="databaseServerPort">Port number for database servers</param>
        /// <param name="routerPortNumber">Port number for router</param>
        public AWSMongoManager(CloudController controller,Amazon.EC2.InstanceType instanceSize,CloudImage image, List<string> securityGroups,int configServerPort = 27019, int databaseServerPort = 27017, int routerPortNumber = 2700, string configServerSet = "config", string databaseServerSet = "database")
        {
            configPort = configServerPort;
            databasePort = databaseServerPort;
            shardPort = routerPortNumber;
            this.controller = controller;
            this.securityGroups = securityGroups;
            this.size = instanceSize;
            this.image = image;

            configClient = new MongoClient("mongodb://127.0.0.1:" + configPort+"?replicaSet="+configServerSet);
            databaseClient = new MongoClient("mongodb://127.0.0.1:" + databasePort+"?replicaSet="+databaseServerSet);
            
        }

        async Task DeployMongoInstance()
        {
            await image.CreateVM(securityGroups, size);

        }

        public async Task<int> GetClusterSize()
        {
            return (await databaseClient.GetDatabase("admin").RunCommandAsync(new BsonDocumentCommand<BsonDocument>(BsonDocument.Parse("replSetGetStatus"))))["members"].AsBsonArray.Count;
        }

        public async Task DeployMongoInstances(int numInstances)
        {
            int currentClusterSize = await GetClusterSize();
            if((currentClusterSize+numInstances) % 2 == 0)
            {
                throw new InvalidOperationException("Number of servers must be odd.");
            }
            Task[] deployments = new Task[numInstances];
            for(int i = 0;i<numInstances;i++)
            {
                deployments[i] = DeployMongoInstance();
            }
            await Task.WhenAll(deployments);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    controller.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }
        

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            
        }
        #endregion

    }
}
