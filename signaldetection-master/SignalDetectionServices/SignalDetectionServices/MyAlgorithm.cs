using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SignalDetectionServices
{
    public class MyAlgorithm : SignalDetectionServices.Functions
    {
        /// <summary>
        /// The ProcessSpectrum method is called for every spectrum.  The argment is a json spectrum object
        /// </summary>
        /// <param name="spectrum"></param>
        /// <returns></returns>
        public override string ProcessSpectrum(JToken spectrum)
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
        /// This is the reducer method that is called after every minion has finished. The 
        /// argment is a stringified json string of all the objects created in the ProcessSpectrum method
        /// </summary>
        /// <param name="json"></param>
        /// <returns>a string</returns>
        public override string Reducer(string json)
        {
            double sum = 0;
            JArray array = JArray.Parse(json);
            foreach (var c in array.Children())
            {
                sum += c.Value<double>();
            }
            return sum.ToString();
        }
    }
}
