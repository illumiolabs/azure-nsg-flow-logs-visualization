/*
Copyright 2019 Illumio, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#load "classes.csx"

#r "Microsoft.WindowsAzure.Storage"
#r "Newtonsoft.Json"

using System;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Text;
using System.Net;

public static async Task Run(CloudBlockBlob myBlob, string subscriptionID, string resourceGroupName, string nsgName, string year, string month, string day, string hour, string macAddress, ILogger log)
{
    log.LogInformation("Uploading NSG Flows to PCE");

    // PCE environment variables.
    string pceAddress = getEnvironmentVariable("PCE_HOSTADDR");
    string pceToken = getEnvironmentVariable("PCE_APITOKEN");
    string pceUser = getEnvironmentVariable("PCE_APIUSER");
    string pceOrg = getEnvironmentVariable("PCE_ORG");

    if (pceAddress.Length == 0 || pceToken.Length == 0 || pceUser.Length == 0 || pceOrg.Length == 0)
    {
        log.LogError("Values for PCE Address, Token, API User and Org are required.");
        return;
    }

    // NSG environment variables.
    // nsgTrafficDecision can be all, allowed or denied. Empty string, no env
    // variables or typo, defaults nsgTrafficDecision to allowed traffic being
    // visualized.
    string nsgTrafficDecision = getEnvironmentVariable("NSG_TRAFFIC_DECISION").ToLower();
    if ( (nsgTrafficDecision.Length == 0) ||
        !(nsgTrafficDecision == "all" ||
          nsgTrafficDecision == "allowed" ||
          nsgTrafficDecision == "denied")) {
        nsgTrafficDecision = "allowed";
    }

    string csvData = "";
    using (var stream = await myBlob.OpenReadAsync())
    using (var sr = new StreamReader(stream))
    using (var jr = new JsonTextReader(sr))
    {
        var result = JsonSerializer.CreateDefault().Deserialize<NSGFlowLogRecords>(jr);
        foreach (var record in result.records)
        {
            float version = record.properties.Version;
            foreach (var outerFlow in record.properties.flows)
            {
                foreach (var innerFlow in outerFlow.flows)
                {
                    foreach (var flowTuple in innerFlow.flowTuples)
                    {
                        var tuple = new NSGFlowLogTuple(flowTuple, version);
                        csvData += getNSGTrafficData(tuple, nsgTrafficDecision);
                    }
                }
            }
        }
    }

    string uri = pceAddress + "api/v2/orgs/" + pceOrg + "/agents/bulk_traffic_flows";
    WebRequest request = WebRequest.Create(uri);
    request.Method = "POST";
    byte[] byteArray = Encoding.UTF8.GetBytes(csvData);
    request.ContentType = "text/csv";
    request.ContentLength = byteArray.Length;

    string authInfo = pceUser + ":" + pceToken;
    authInfo = Convert.ToBase64String(Encoding.Default.GetBytes(authInfo));
    request.Headers["Authorization"] = "Basic " + authInfo;

    Stream dataStream = request.GetRequestStream();
    dataStream.Write(byteArray, 0, byteArray.Length);
    dataStream.Close();

    WebResponse response = request.GetResponse();
    log.LogInformation(((HttpWebResponse)response).StatusDescription);

    using (dataStream = response.GetResponseStream())
    {
        StreamReader reader = new StreamReader(dataStream);
        string responseFromServer = reader.ReadToEnd();
        log.LogInformation(responseFromServer);
    }

    response.Close();
    log.LogInformation($"Uploaded flow to: {uri}");
}

static string getNSGTrafficData(NSGFlowLogTuple tuple, string nsgTrafficDecision) {
    string data = "";

    if (nsgTrafficDecision == "all") {
        data += getTupleData(tuple);
    } else if (nsgTrafficDecision == "denied") {
        if (tuple.deviceAction == "D") {
            data += getTupleData(tuple);
        }
    } else {
        if (tuple.deviceAction == "A") {
            data += getTupleData(tuple);
        }    
    }

    return data;
}

static string getTupleData(NSGFlowLogTuple tuple) {
    var temp = new StringBuilder();
    temp.Append(tuple.sourceAddress).Append(",");
    temp.Append(tuple.destinationAddress).Append(",");
    temp.Append(tuple.destinationPort).Append(",");
    temp.Append(tuple.transportProtocol == "U" ? "17" : "6");
    temp.Append(Environment.NewLine);
    return temp.ToString();
}

public static string getEnvironmentVariable(string name)
{
    var result = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    if (result == null)
        return "";
        
    return result; 
}
