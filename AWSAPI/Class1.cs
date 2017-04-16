using Amazon.EC2;
using Amazon.EC2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSAPI
{
    namespace Infrastructure
    {

        namespace Virtualization
        {
            public class VMAsset
            {
                internal AmazonEC2Client client;
                internal VMAsset(AmazonEC2Client client)
                {
                    this.client = client;
                }
            }

            public enum VMState
            {
                /// <summary>
                /// The virtual machine is running
                /// </summary>
                Running,
                /// <summary>
                /// The virtual machine is in a STOPPED or ERROR state
                /// </summary>
                Stopped,
                /// <summary>
                /// The virtual machine is starting
                /// </summary>
                Starting,
                /// <summary>
                /// The state of the virtual machine is unknown
                /// </summary>
                Unknown
            }
            public class VirtualMachine : VMAsset
            {
                public async Task Start()
                {
                    await client.StartInstancesAsync(new StartInstancesRequest(new List<string>(new string[] { instance.InstanceId })));

                }
                public async Task Stop()
                {
                    await client.StopInstancesAsync(new StopInstancesRequest(new List<string>(new string[] { instance.InstanceId })));

                }
                public async Task Delete()
                {
                    await client.TerminateInstancesAsync(new TerminateInstancesRequest(new List<string>(new string[] { instance.InstanceId })));

                }
                Instance instance;
                internal VirtualMachine(AmazonEC2Client client, Instance instance) : base(client)
                {
                    this.instance = instance;

                }
                /// <summary>
                /// Retrieves the private IP of this instance
                /// </summary>
                public string PrivateIP
                {
                    get
                    {
                        return instance.PrivateIpAddress;
                    }
                }
                /// <summary>
                /// Retrieves the public IP of this instance
                /// </summary>
                public string PublicIP
                {
                    get
                    {
                        return instance.PublicIpAddress;
                    }
                }
                public VMState State
                {
                    get
                    {
                        int lowByte = (byte)instance.State.Code;
                        switch (lowByte)
                        {
                            case 0:
                                return VMState.Starting;
                            case 16:
                                return VMState.Running;
                            case 32:
                                return VMState.Stopped;
                            case 80:
                                return VMState.Stopped;
                            default:
                                return VMState.Unknown;
                        }
                    }
                }
            }
            public class CloudImage:VMAsset
            {
                /// <summary>
                /// Creates a new virtual machine with the specified firewall configuration
                /// </summary>
                /// <param name="securityGroups">List of security group IDs (typically started with sg-) to provision this VM with</param>
                /// <param name="size">The size of the virtual machine to provision</param>
                /// <returns></returns>
                public async Task<VirtualMachine> CreateVM(List<string> securityGroups, InstanceType size)
                {
                    RunInstancesRequest request = new RunInstancesRequest(awsImage.ImageId, 1, 1);
                    request.SecurityGroupIds = securityGroups;
                    request.InstanceType = size;
                    var response = await client.RunInstancesAsync(request);
                    return new VirtualMachine(client, response.Reservation.Instances.First());
                }
                Image awsImage;
                public CloudImage(Image image, AmazonEC2Client client):base(client)
                {
                    awsImage = image;
                }
                public string Description
                {
                    get
                    {
                        return awsImage.Description;
                    }
                }
            }
            public class CloudController:IDisposable
            {
                AmazonEC2Client client;
                public CloudController(string accessKey, string secretKey, Amazon.RegionEndpoint region)
                {
                    client = new AmazonEC2Client(accessKey, secretKey, region);
                }
                public async Task<IEnumerable<VirtualMachine>> GetVirtualMachinesByPrivateIps(IEnumerable<string> ips)
                {
                   return  (await client.DescribeInstancesAsync(new DescribeInstancesRequest() { Filters = new List<Filter>(new Filter[] { new Filter("private-ip-address", new List<string>(ips)) }) })).Reservations.SelectMany(m=>m.Instances).Select(m=>new VirtualMachine(client,m));
                }

                public async Task<IEnumerable<CloudImage>> GetImages()
                {
                    return (await client.DescribeImagesAsync(new DescribeImagesRequest() { Owners = new List<string>(new string[] { "self" }) })).Images.Select(m => new CloudImage(m,client));
                }

                #region IDisposable Support
                private bool disposedValue = false; // To detect redundant calls

                protected virtual void Dispose(bool disposing)
                {
                    if (!disposedValue)
                    {
                        if (disposing)
                        {
                            client.Dispose();
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
    }
}
