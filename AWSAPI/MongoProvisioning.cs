using AWSAPI.Infrastructure.Virtualization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Net;
using System.Net.Sockets;
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
        UdpClient receiver;

        /// <summary>
        /// Creates a new management instance
        /// </summary>
        /// <param name="configServerPort">Port number for configuration servers</param>
        /// <param name="databaseServerPort">Port number for database servers</param>
        /// <param name="routerPortNumber">Port number for router</param>
        public AWSMongoManager(CloudController controller,Amazon.EC2.InstanceType instanceSize,CloudImage image, List<string> securityGroups,int configServerPort = 27019, int databaseServerPort = 27017, int routerPortNumber = 2700, string configServerSet = "config", string databaseServerSet = "database", int transponderPort = 9090)
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
            receiver = new UdpClient(new IPEndPoint(IPAddress.Any, transponderPort));
            receiver.JoinMulticastGroup(IPAddress.Parse("239.255.255.250"));
            
        }
        Dictionary<IPAddress, List<TaskCompletionSource<bool>>> waitingCommands = new Dictionary<IPAddress, List<TaskCompletionSource<bool>>>();
        

        

        async Task<VirtualMachine> DeployVM()
        {
            return await image.CreateVM(securityGroups, size);
        }
        

        public async Task<BsonDocument> GetClusterConfiguration(MongoClient client)
        {
            return (await client.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetGetConfig = 1 })))["config"].AsBsonDocument;
        }
        public async Task<BsonArray> GetClusterMembers()
        {
            return (await databaseClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetGetStatus = 1 })))["members"].AsBsonArray;
        }
        public async Task<int> GetClusterSize()
        {
            return (await databaseClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetGetStatus = 1 })))["members"].AsBsonArray.Count;
        }
        void AddMember(BsonDocument config, VirtualMachine vm, int port)
        {
            var members = config["members"].AsBsonArray;
            config["members"].AsBsonArray.Add(new { _id = members.OrderByDescending(m=>m["_id"].AsInt32).Select(m=>m["_id"].AsInt32).FirstOrDefault()+1, host = vm.PrivateIP+":"+port }.ToBsonDocument());
            config["version"] = config["version"].AsInt32 + 1;
        }

        void RemoveMember(BsonDocument config, BsonValue member)
        {
            var members = config["members"].AsBsonArray;
            config["members"].AsBsonArray.Remove(member);
            config["version"] = config["version"].AsInt32 + 1;
        }

        public async Task DeployMongoInstances(int numInstances)
        {
            int currentClusterSize = await GetClusterSize();
            if((currentClusterSize+numInstances) % 2 == 0)
            {
                throw new InvalidOperationException("Number of servers must be odd.");
            }

            //Perform provisioning operation (AWS)
            Task<VirtualMachine>[] deployments = new Task<VirtualMachine>[numInstances];
            for(int i = 0;i<numInstances;i++)
            {
                deployments[i] = DeployVM();
            }
            await Task.WhenAll(deployments);

            //Re-configure database (assumes exclusive lock)
            BsonDocument databaseConfig = await GetClusterConfiguration(databaseClient);
            BsonDocument configconfig = await GetClusterConfiguration(configClient);
            foreach (var iable in deployments)
            {
                AddMember(databaseConfig, iable.Result, databasePort);
                AddMember(configconfig, iable.Result, configPort);
                
            }
            while (true)
            {
                try
                {
                    await databaseClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetReconfig = databaseConfig }));
                    break;
                }catch(Exception er)
                {
                 
                }
            }
            await configClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetReconfig = configconfig }));
            
        }
        /// <summary>
        /// Undeploys the specified number of instances
        /// </summary>
        /// <param name="numInstances">The number of instances to remove</param>
        /// <param name="allowDisaster">Whether or not to allow a takedown of the entire cluster.</param>
        /// <returns></returns>
        public async Task UndeployMongoInstances(int numInstances, bool allowDisaster = false)
        {
            BsonDocument cconfig = await GetClusterConfiguration(configClient);
            BsonDocument dbconfig = await GetClusterConfiguration(databaseClient);
            BsonArray members = cconfig["members"].AsBsonArray;
            int currentClusterSize = members.Count;
            if(currentClusterSize-numInstances <= 0 && !allowDisaster)
            {
                throw new InvalidOperationException("Requested operation would yield this disastrous result: https://aws.amazon.com/message/41926/");
            }
            if ((currentClusterSize - numInstances) % 2 == 0)
            {
                throw new InvalidOperationException("Number of servers must be odd.");
            }
            
            BsonDocument databaseConfig = await GetClusterConfiguration(databaseClient);
            BsonDocument configconfig = await GetClusterConfiguration(configClient);
            databaseConfig["members"].AsBsonArray.Where(m=>!System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces().SelectMany(a=>a.GetIPProperties().UnicastAddresses).Select(a=>a.Address.ToString()).Contains(m["host"].ToString().Replace(":"+databasePort,""))).Take(numInstances).ToList().AsParallel().ForAll(m=>RemoveMember(databaseConfig,m));
            var configTakedowns = configconfig["members"].AsBsonArray.Where(m => !System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces().SelectMany(a => a.GetIPProperties().UnicastAddresses).Select(a => a.Address.ToString()).Contains(m["host"].ToString().Replace(":" + configPort, ""))).Take(numInstances).ToList();
            configTakedowns.AsParallel().ForAll(m => RemoveMember(configconfig, m));

            await configClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetReconfig = configconfig }));
            await databaseClient.GetDatabase("admin").RunCommandAsync(new ObjectCommand<BsonDocument>(new { replSetReconfig = databaseConfig }));

            var pendingShutdowns = await controller.GetVirtualMachinesByPrivateIps(configTakedowns.Select(m => m["host"].ToString().Replace(":" + configPort, "")));
            await Task.WhenAll(pendingShutdowns.Select(m => m.Delete()));
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
