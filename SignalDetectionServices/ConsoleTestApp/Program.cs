using Amazon.Lambda.DynamoDBEvents;
using Amazon.Runtime;
using Amazon.S3;

using Microsoft.Extensions.Configuration;
using SignalDetectionServices;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static Amazon.Lambda.DynamoDBEvents.DynamoDBEvent;
/*
{

"Bucket" :"asterisk-rawspectra-collections",
"MeasurementId" : "121f9f87-ad18-421d-9a9d-e15568e3f648.json",
"FirstScan" : "1",
"Chunk" : 200
}
*/
namespace ConsoleTestApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Task task = new Task(Process);
            task.Start();
            task.Wait();
            Console.ReadLine();
        }

        private static AmazonS3Client MakeS3Client()
        {
            var config = new ConfigurationBuilder()
                    .AddUserSecrets("Xtopia1")
                    .Build();
            var accessKey = config["aws-access-key"];
            var secretKey = config["aws-secret-key"];
            var credentials = new BasicAWSCredentials(
                                    accessKey: accessKey,
                                    secretKey: secretKey);
            var S3Client = new AmazonS3Client(credentials, Amazon.RegionEndpoint.USEast1);
            return S3Client;
        }
         
        public static async void Process()
        {
            EventObject evt = new EventObject()
            {
                Bucket = "asterisk-rawspectra-collections",
                MeasurementId = "121f9f87-ad18-421d-9a9d-e15568e3f648.json",
                FirstScan = 22,
                LastScan = 343,
                Chunk = 500,
                Id = Guid.NewGuid().ToString(),
                NumberOfPieces = 1
            };
            // these have to be set manually for each deployment
            Environment.SetEnvironmentVariable("NumberOfScans", "michael-experiment-NumberOfScans-P1L84QWVH6S4");
            Environment.SetEnvironmentVariable("ProcessSpectrumContoller", "michael-experiment-ProcessSpectrumContoller-5KCIY6EO98LS");
            Environment.SetEnvironmentVariable("DDBTableName", "michael-experiment2-DDBTable-OF37AGTUXLG5");
            // use local s3 client
            MyAlgorithm f = new MyAlgorithm();
          
            // test the individual spectrum processor
            //var x = await f.ProcessSpectrumContollerLambda(evt, null);

            // test the full file analysis
            var result = await f.ProcessSpectraAsyncLambda(evt, null);
            Console.WriteLine(result);

            // test the reducer
            //DynamoDBEvent ddbevt = new DynamoDBEvent();
            //var rec = new DynamoDBEvent.DynamodbStreamRecord();
            //rec.Dynamodb = new Amazon.DynamoDBv2.Model.StreamRecord();
            //rec.Dynamodb.Keys.Add("scan", new Amazon.DynamoDBv2.Model.AttributeValue("34.333"));
            //rec.Dynamodb.Keys.Add("id", new Amazon.DynamoDBv2.Model.AttributeValue("4952af83-acb3-43d5-8ccd-ceeee7a2f7a7"));
            //ddbevt.Records = new List<DynamodbStreamRecord>();
            //ddbevt.Records.Add(rec);
            // xx = await f.OnNewDBRecord(ddbevt, null);
        }

        
    }
}
