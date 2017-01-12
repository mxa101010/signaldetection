using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using System.IO;
using Newtonsoft.Json.Linq;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Lambda.Model;
using Amazon.Lambda;
using Amazon.DynamoDBv2;

using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Collections;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializerAttribute(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace SignalDetectionServices
{

    public class EventObject
    {
        public string Bucket { get; set; }
        public string MeasurementId { get; set; }
        public int FirstScan { get; set; }
        public int LastScan { get; set; }
        public int Chunk { get; set; }
        public string Id { get; set; }
        public int NumberOfPieces { get; set; }
        public int PieceNumber { get; set; }
    }



    public class Functions
    {

        #region Constructor
        private AmazonS3Client S3Client = null;
        // <summary>
        // Default constructor.This constructor is used by Lambda to construct the instance.When invoked in a Lambda environment
        // the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        // region the Lambda function is executed in.
        // </summary>
        public Functions( Amazon.RegionEndpoint region)
        {
            Region = region;
            ConnectToS3();
        }

        public Functions()
        {
            Region = Amazon.RegionEndpoint.USEast1;
            ConnectToS3();
        }

        private void ConnectToS3()
        {
            try
            {
                S3Client = new AmazonS3Client(Region);
            }
            catch (Exception ex)
            {
                Log(null, ex.Message);
            }
        }

        /// <summary>
        /// AWS Region - default is USEast1
        /// </summary>
        public Amazon.RegionEndpoint Region { get; set; }

        // <summary>
        // Constructs an instance with a preconfigured S3 client.This can be used for testing the outside of the Lambda environment.
        // </summary>
        // <param name = "s3Client" ></ param >


        #endregion

        #region Virtual methods - algorithms go here
        /// <summary>
        /// Application specific function to process a single spectrum
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public virtual string ProcessSpectrum(JToken spectrum)
        {
            // calculate TIC the hard way
            string result = "";
            var intensities = spectrum["Intensities"].Value<JArray>();
            double sum = 0;
            for (int i = 0; i < intensities.Count; i++)
            {
                sum += intensities[i].Value<double>();
            }
            result += sum.ToString() + ",";

            return result;
        }
        /// <summary>
        /// User supplied function to process the json array of objects constructed
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public virtual string Reducer(string json)
        {
            double sum = 0;
            JArray array = JArray.Parse(json);
            foreach (var c in array.Children())
            {
                sum += c.Value<double>();
            }
            return sum.ToString();
        }

        #endregion

        #region Lambda functions
        public async Task<string> OnNewDBRecordLambda(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            //Log(context, "OnNewDBRecord Request: " + JsonConvert.SerializeObject(dynamoEvent));
            // the id of the object
            string id = dynamoEvent.Records[0].Dynamodb.Keys["id"].S;
            // the convention of the scan is <scan number>.<total number of scans>
            var scan = dynamoEvent.Records[0].Dynamodb.Keys["scan"];
            var numberofpieces = Convert.ToInt32(scan.S.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries)[1]);
            var piecenumber = Convert.ToInt32(scan.S.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries)[0]);
            Log(context, "OnNewDDB: Piece " + piecenumber + " of " + numberofpieces);
            // dont bother to execute the query unless you are one of the last five pieces
            if (piecenumber < numberofpieces - 5) return "BYPASS";
            // build a client
            AmazonDynamoDBClient client = null;
            if (context == null)
            {
                client = new AmazonDynamoDBClient(Region);
            } else
            {
                client = new AmazonDynamoDBClient();
            }
            // get the table name from the environment
            var tablename = System.Environment.GetEnvironmentVariable("DDBTableName");

            // count records w/o returning them to the client
            QueryRequest qr = new QueryRequest(tablename);
            qr.KeyConditionExpression = "id = :v_Id";
            qr.ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                 {":v_Id", new AttributeValue { S =  id}}
            };
            qr.Select = Select.COUNT;
            var  qcount = await client.QueryAsync(qr);

            if( qcount.Count == numberofpieces)
            {
                Log(context, "******Finished " + numberofpieces);
                // get all the records
                Table table = Table.LoadTable(client, tablename);
                QueryFilter queryFilter = new QueryFilter();
                queryFilter.AddCondition("id", QueryOperator.Equal, id);

                Search alternativeIdSearch = table.Query(queryFilter);
                List<Document> docs = await alternativeIdSearch.GetNextSetAsync();
                string result = "[";
                foreach( var d in docs )
                {
                    result += d["item"];
                }
                result = result.Substring(0, result.Length - 1) + "]";
                string reduced = Reducer(result);
                Log(context, reduced);
            } else
            {
                Log(context, "Complete records: " + qcount.Count);
            }
            return "OK";
        }
 
        public async Task<string> ProcessSpectrumContollerLambda(EventObject input, ILambdaContext context)
        {
            try
            {
                // first get the index
                string json = await ReadFile(input.Bucket, "indexes/" + input.MeasurementId);
                JArray jsona = JArray.Parse(json);
                var pick = jsona.Children<JObject>();
                var nitems = pick.Count();
                var startitem = pick.ElementAt<JObject>(input.FirstScan);
                var lastitem = pick.ElementAt<JObject>(input.LastScan);
                var xstart = startitem["start"].Value<int>();
                var xstop = lastitem["end"].Value<int>();// + input.LastScan - input.FirstScan;
                // now get the spectrum
                string jspec = await ReadBlock(input.Bucket, input.MeasurementId, xstart, xstop);
                var spec = JArray.Parse("["+jspec+"]");
                int scan = input.FirstScan;
                string result = "";
                foreach (var s in spec.Children())
                {
                    result += ProcessSpectrum(s);

                }
                string status = await WriteObject(input.Id, input.PieceNumber, input.NumberOfPieces, result, context);
                if (!status.Equals("OK")) Log(context, status);
                Log(context, "Processed " + input.FirstScan);
                return status;

            }
            catch (Exception e)
            {
                Log(context, $"Error getting object {input.MeasurementId} from bucket {input.Bucket}. Make sure they exist and your bucket is in the same region as this function.");
                Log(context, e.Message);
                Log(context, e.StackTrace);
                throw;
            }
        }

        public async Task<string> ProcessSpectraAsyncLambda(EventObject input, ILambdaContext context)
        {

            // if it is executed on AWS, let AWS figure out the endpoint
            AmazonLambdaClient lam = null;
            if (context == null)
            {
                lam = new AmazonLambdaClient(Region);
            }
            else
            {
                lam = new AmazonLambdaClient();
            }
            string json0 = JsonConvert.SerializeObject(input);

            int nscans = 0;
            try
            {
                // first get the index
                string json = await ReadFile(input.Bucket, "indexes/" + input.MeasurementId);
                JArray jsona = JArray.Parse(json);
                var pick = jsona.Children<JObject>();
                nscans = pick.Count();
            }
            catch (Exception e)
            {
                Log(context, e.Message);
                Log(context, e.StackTrace);
                throw;
            }
            Log(context, "Found " + nscans + " scans" + System.Environment.NewLine);

            // this is the list of things
            ConcurrentBag<string> aggregate = new ConcurrentBag<string>();
            int CHUNK = input.Chunk;
            if (CHUNK == 0) CHUNK = 200;
            input.Id = Guid.NewGuid().ToString();
            List<int> scans = new List<int>();
            for (int i = 1; i < nscans; i = i + CHUNK) scans.Add(i);
            input.NumberOfPieces = scans.Count;

            for(int i=0;i<scans.Count; i++)
            {
                // update the payload request
                input.FirstScan = scans[i];
                input.LastScan = (int)Math.Min(scans[i] + CHUNK, nscans - 1);
                input.PieceNumber = i+1;
                string json = JsonConvert.SerializeObject(input);
                InvokeRequest ir = new InvokeRequest()
                {
                    FunctionName = System.Environment.GetEnvironmentVariable("ProcessSpectrumContoller"),
                    InvocationType = InvocationType.Event,
                    Payload = json
                };
                //Log(context, "Iteration - " + json+System.Environment.NewLine);
                try
                {
                    // don't wait for a response
                    var lambda = lam.InvokeAsync(ir);
                    var result = await lambda;
                }
                catch (Exception e)
                {
                    Log(context, e.Message);
                    Log(context, e.StackTrace);
                    //  throw;
                }
            }

            Log(context, "Finished scheduling minuons" + aggregate.Count);
            
            return "OK";
        }
        #endregion

        #region OBSOLETE - sample gateway api lambda
        /*
        /// <summary>
        /// A Lambda function to respond to HTTP Get methods from API Gateway
        /// </summary>
        /// <param name="request"></param>
        /// <returns>The list of blogs</returns>
        public APIGatewayProxyResponse Get(APIGatewayProxyRequest request, ILambdaContext context)
        {
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(Region);
            Log(context, "Get Request: ");
            var tablename = System.Environment.GetEnvironmentVariable("DDBTableName");
            //Log(context, "Table name " + tablename);
            ScanFilter scanFilter = new ScanFilter();
            Table table = Table.LoadTable(client, tablename);
       


            QueryOperationConfig config = new QueryOperationConfig()
            { 
                IndexName = "id", //Partition key
                AttributesToGet = new List<string>
                { "scan" },
                ConsistentRead = true
            };

            Search search = table.Query(config);
            Log(context,"Found "+search.Matches.Count());
            //var task = FunctionHandler(evt, null);
            //var result = task;
            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = "GET1 Hello AWS Serverless",
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };

            return response;
        }
        */
        #endregion

        #region OBSOLETE - synchronous lambda scheduling
        /*
        public async Task<string> OBSOLETEProcessSpectra(EventObject input, ILambdaContext context)
        {

            // if it is executed on AWS, let AWS figure out the endpoint
            AmazonLambdaClient lam = null;
            if (context == null)
            {
                lam = new AmazonLambdaClient(Region);
            }
            else
            {
                lam = new AmazonLambdaClient();
            }
            string json0 = JsonConvert.SerializeObject(input);
            // get list of scans for this measurement
            InvokeRequest irscans = new InvokeRequest()
            {
                // get the function name from environment
                FunctionName = System.Environment.GetEnvironmentVariable("NumberOfScans"),
                InvocationType = InvocationType.RequestResponse,
                Payload = json0
            };
            //Log(context, "Function " + irscans.FunctionName + System.Environment.NewLine);
            int nscans = 0;
            try
            {

                var lambda = lam.InvokeAsync(irscans);
                var result = await lambda;
                var bytes = result.Payload.ToArray();
                string sval = System.Text.Encoding.ASCII.GetString(bytes);
                sval = sval.Replace('"', ' ');
                nscans = Convert.ToInt32(sval);
            }
            catch (Exception e)
            {
                Log(context, e.Message);
                Log(context, e.StackTrace);
                throw;
            }
            Log(context, "Found " + nscans + " scans" + System.Environment.NewLine);

            // this is the list of things
            ConcurrentBag<string> aggregate = new ConcurrentBag<string>();
            int CHUNK = input.Chunk;
            if (CHUNK == 0) CHUNK = 200;
            input.Id = Guid.NewGuid().ToString();
            List<int> scans = new List<int>();
            for (int i = 1; i < nscans; i=i+CHUNK) scans.Add(  i);
            input.NumberOfPieces = scans.Count;

            await Task.WhenAll(scans.Select(async (i) =>
            {
               // update the payload request
               input.FirstScan = i;
               input.LastScan = (int)Math.Min(i + CHUNK, nscans-1);
               string json = JsonConvert.SerializeObject(input);
               InvokeRequest ir = new InvokeRequest()
               {
                   FunctionName = System.Environment.GetEnvironmentVariable("ProcessSpectrumContoller"),
                   InvocationType = InvocationType.RequestResponse,
                   Payload = json
               };
                   //Log(context, "Iteration - " + json+System.Environment.NewLine);
                   try
               {
                   var lambda = lam.InvokeAsync(ir);
                   var result = await lambda;
                   var bytes = result.Payload.ToArray();
                   var val = System.Text.Encoding.ASCII.GetString(bytes);
                   val = val.Replace('"', ' ').Trim();
                   //Log(context, "Value " + val + System.Environment.NewLine);
                   aggregate.Add(val);
               }
               catch (Exception e)
               {
                   Log(context, e.Message);
                   Log(context, e.StackTrace);
                   //  throw;
               }
           }));

            Log(context, "Finished " + aggregate.Count);
            string jresult = "[";
            for (int i = 0; i < aggregate.Count; i++)
            {
                jresult += aggregate.ElementAt(i);
                //Log(context, "Item " + aggregate.ElementAt(i).ToString());
            }
            jresult = jresult.Trim();
            jresult = jresult.Substring(0, jresult.Length - ",".Length);
            jresult += "]";
            return jresult;
        
    */
        #endregion

        #region OBSOLETE - count scans
        //public async Task<string> NumberOfScansLambda(EventObject input, ILambdaContext context)
        //{
        //    try
        //    {
        //        // first get the index
        //        string json = await ReadFile(input.Bucket, "indexes/" + input.MeasurementId);
        //        JArray jsona = JArray.Parse(json);
        //        var pick = jsona.Children<JObject>();
        //        var nitems = pick.Count();

        //        return nitems.ToString();
        //    }
        //    catch (Exception e)
        //    {
        //        Log(context, e.Message);
        //        Log(context, e.StackTrace);
        //        throw;
        //    }
        //}
        #endregion

        #region Utility Methods
        /// <summary>
        /// Functions referencing this method need to define the DDBTableName environment variable
        /// </summary>
        /// <param name="id"></param>
        /// <param name="scan"></param>
        /// <param name="totalscans"></param>
        /// <param name="item"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private async Task<string> WriteObject(string id, int scan, int totalscans, string item, ILambdaContext context)
        {
            // build a client
            AmazonDynamoDBClient client = null;
            try
            {
                if (context == null)
                {
                    client = new AmazonDynamoDBClient(Region);
                }
                else
                {
                    client = new AmazonDynamoDBClient();
                }
                // get the table name from the environment
                var tablename = System.Environment.GetEnvironmentVariable("DDBTableName");
                //Log(context, "Table name " + tablename);
                Table table = Table.LoadTable(client, tablename);
                Document d = new Document();
                d["id"] = id;
                d["scan"] = scan.ToString() + "." + totalscans.ToString();
                d["item"] = item;
                await table.PutItemAsync(d);
                return "OK";
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }

        private async Task<string> SendSNS(string message)
        {
            AmazonSimpleNotificationServiceClient client = new AmazonSimpleNotificationServiceClient(Region);
            PublishRequest req = new PublishRequest();
            req.Message = message;
            req.Subject = "status";
            req.TopicArn = System.Environment.GetEnvironmentVariable("SNSTopicARN");
            var result = await client.PublishAsync(req);
            return result.HttpStatusCode.ToString();
        }

        private async void Log(ILambdaContext context, string msg)
        {
            if (context == null)
            {
                Console.WriteLine(msg);
            }
            else
            {
                context.Logger.LogLine(msg);
            }
            try
            {
                await SendSNS(msg);
            } catch(Exception ex)
            {
                context.Logger.LogLine(ex.Message);
            }
        }
        public async Task<string> ReadFile(string bucket, string key)
        {
            using (var response = await S3Client.GetObjectAsync(bucket, key))
            {
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    return await reader.ReadToEndAsync();
                }
            }
        }

        public async Task<string> ReadBlock(string bucket, string key, int start, int end)
        {
            GetObjectRequest req = new GetObjectRequest();
            req.BucketName = bucket;
            req.ByteRange = new ByteRange(start, end);
            req.Key = key;
            using (var response = await S3Client.GetObjectAsync(req))
            {
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    return await reader.ReadToEndAsync();
                }
            }
        }
        #endregion
    }
}
