using System.Threading.Tasks;
using Amazon.EC2;
using Amazon.ElasticMapReduce;
using aws_emr_cost;
using NUnit.Framework;
using Serilog;

namespace tests
{
    public class EmrCostCalculatorTest
    {

        [Test]
        public async Task Test1()
        {
            var emrClient = new AmazonElasticMapReduceClient();
            var ec2Client = new AmazonEC2Client();;
            
            var logger = Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .CreateLogger();
            
            var emrCostCalculator = new EmrCostCalculator(emrClient, ec2Client, logger);
            
            await emrCostCalculator.GetClusterCost("j-9A0BZEK44EPV");

            Assert.Pass();
        }
    }
}