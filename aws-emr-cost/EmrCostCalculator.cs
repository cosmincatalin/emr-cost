using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using Serilog;
using Instance = Amazon.ElasticMapReduce.Model.Instance;
using MarketType = Amazon.ElasticMapReduce.MarketType;

namespace aws_emr_cost
{
    public class EmrCostCalculator
    {
        private ILogger _logger;
        private readonly IAmazonEC2 _ec2Client;
        private readonly IAmazonElasticMapReduce _emrClient;

        private ConcurrentDictionary<(string, string), Dictionary<DateTime, float>> _allPrices = new ConcurrentDictionary<(string, string), Dictionary<DateTime, float>>();

        public EmrCostCalculator(IAmazonElasticMapReduce emrClient, IAmazonEC2 ec2Client, ILogger logger)
        {
            _emrClient = emrClient;
            _ec2Client = ec2Client;
            _logger = logger;
        }

        public async Task GetClusterCost(string clusterId)
        {
            _logger.Information($"Getting information about {clusterId}");
            var cluster = await GetCluster(clusterId);

            var availabilityZone = cluster.Ec2InstanceAttributes.Ec2AvailabilityZone;
            _logger.Information($"Availability zone of the cluster is {availabilityZone}");

            if (cluster.InstanceCollectionType == InstanceCollectionType.INSTANCE_GROUP)
            {
                _logger.Information("Cluster is configured as an Instance Group");
                var response = _emrClient.ListInstanceGroupsAsync(new ListInstanceGroupsRequest
                {
                    ClusterId = clusterId
                });
                var groups = response.Result.InstanceGroups;
                foreach (var group in groups)
                {
                    _logger.Information($"Getting information about group {group.Name}");
                    var instances = await GetInstances(group, clusterId);
                    var costs = await Task.WhenAll(instances.Select(async instance => await GetInstanceCost(instance, availabilityZone)));
                    var cost = costs.Aggregate(.0, (acc, x) => acc + x);
                    _logger.Information($"{cost:F2}$");
                }
            }
            
            
        }

        private async Task<IEnumerable<Instance>> GetInstances(InstanceGroup instanceGroup, string clusterId)
        {
            var response = await _emrClient.ListInstancesAsync(new ListInstancesRequest
            {
                ClusterId = clusterId,
                InstanceGroupId = instanceGroup.Id
            });

            return response
                .Instances;
        }

        private async Task<double> GetInstanceCost(Instance instance, string availabilityZone)
        {
            if (instance.Market == MarketType.SPOT)
            {
                _logger.Information($"Need to look into the Spot Market");
                var cost = await GetBilledPriceForPeriod(instance, availabilityZone);
                return cost;
            }
            else
            {
                return await Task.FromResult(0.0);
            }
        }
        
        private async Task<Cluster> GetCluster(string clusterId)
        {
            var response = await _emrClient.DescribeClusterAsync(new DescribeClusterRequest
            {
                ClusterId = clusterId
            });
            return response.Cluster;
        }
        
        private async Task PopulateAllPricesIfNeeded(Instance instance, string availabilityZone)
        {
            
            var prices = new Dictionary<DateTime, float>();

            if (_allPrices.ContainsKey((instance.InstanceType, availabilityZone)))
            {
                prices = _allPrices[(instance.InstanceType, availabilityZone)];
                if (
                    prices.OrderBy(x => x.Key).First().Key <= instance.Status.Timeline.CreationDateTime &&
                    prices.OrderBy(x => x.Key).Reverse().First().Key >= instance.Status.Timeline.EndDateTime
                    ) 
                {
                    _logger.Information($"Already have the prices cached in memory for {instance.InstanceType}");
                    return;
                }
            }
            
            var nextToken = "";
            while (true)
            {
                _logger.Information($"Requesting prices for {instance.InstanceType}");
                var response= await _ec2Client
                    .DescribeSpotPriceHistoryAsync(new DescribeSpotPriceHistoryRequest 
                    {
                        InstanceTypes = {instance.InstanceType},
                        ProductDescriptions = {"Linux/UNIX (Amazon VPC)"},
                        AvailabilityZone = availabilityZone,
                        StartTimeUtc = instance.Status.Timeline.CreationDateTime,
                        EndTimeUtc = instance.Status.Timeline.EndDateTime,
                        NextToken = nextToken 
                    });
                
                nextToken = response.NextToken;

                response
                    .SpotPriceHistory
                    .ForEach(price =>
                    {
                        prices[price.Timestamp] = float.Parse(price.Price, CultureInfo.InvariantCulture.NumberFormat);
                        _logger.Information($"{price.Timestamp}");
                    });

                if (nextToken == "")
                {
                    break;
                }

            }

            _allPrices[(instance.InstanceType, availabilityZone)] = prices;
        }

        private async Task<double> GetBilledPriceForPeriod(Instance instance, string availabilityZone)
        {
            await PopulateAllPricesIfNeeded(instance, availabilityZone);
            var prices = _allPrices[(instance.InstanceType, availabilityZone)];
            var summedPrice = .0;
            var summedUntilTimestamp = instance.Status.Timeline.CreationDateTime;
            var indexedPrices =  prices.OrderBy(x => x.Key).ToArray();
            // _logger.Information($"{indexedPrices[0].Key}: {indexedPrices[0].Value}");
            // for (var i = 0; i < indexedPrices.Length;)
            // {
            //     TimeSpan secondsPassed;
                // if (i == indexedPrices.Length - 1 || instance.Status.Timeline.EndDateTime < indexedPrices[i + 1].Key)
                // {
                //     _logger.Information($"Summing up partial cost");
                //     secondsPassed = instance.Status.Timeline.EndDateTime - summedUntilTimestamp;
                //     summedPrice += secondsPassed.Seconds * indexedPrices[i].Value / 3600;
                // }
                // if (indexedPrices[i].Key <= summedUntilTimestamp && summedUntilTimestamp < indexedPrices[i + 1].Key)
                // {
                //     _logger.Information($"Summing up partial cost");
                //     secondsPassed = indexedPrices[i + 1].Key - summedUntilTimestamp;
                //     summedPrice += secondsPassed.Seconds * indexedPrices[i].Value / 3600;
                //     summedUntilTimestamp = indexedPrices[i + 1].Key;   
                // }
                // _logger.Information($"{indexedPrices[i].Key}");
            // }

            return summedPrice;
        }
    }
}