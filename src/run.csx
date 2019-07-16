#r "Microsoft.WindowsAzure.Storage"
#r "Newtonsoft.Json"

using System;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Buffers;
using System.IO;
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
                        // display only accepted flows for now.
                        if (tuple.deviceAction == "A") {
                            var temp = new StringBuilder();
                            temp.Append(tuple.sourceAddress).Append(",");
                            temp.Append(tuple.destinationAddress).Append(",");
                            temp.Append(tuple.destinationPort).Append(",");
                            temp.Append(tuple.transportProtocol == "U" ? "17" : "6");
                            temp.Append(Environment.NewLine);
                            csvData += temp.ToString();
                        }
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

public static string getEnvironmentVariable(string name)
{
    var result = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    if (result == null)
        return "";
        
    return result; 
}

// The code following this comment is borrowed from
// https://github.com/microsoft/AzureNetworkWatcherNSGFlowLogsConnector/blob/2e21dfa55dec2dcea6deaf1d293e788cc0177822/NwNsgProject/classes.cs#L282-L473

class NSGFlowLogTuple
{
    public float schemaVersion { get; set; }

    public string startTime { get; set; }
    public string sourceAddress { get; set; }
    public string destinationAddress { get; set; }
    public string sourcePort { get; set; }
    public string destinationPort { get; set; }
    public string transportProtocol { get; set; }
    public string deviceDirection { get; set; }
    public string deviceAction { get; set; }

    // version 2 tuple properties
    public string flowState { get; set; }
    public string packetsStoD { get; set; }
    public string bytesStoD { get; set; }
    public string packetsDtoS { get; set; }
    public string bytesDtoS { get; set; }

    public NSGFlowLogTuple(string tuple, float version)
    {
        schemaVersion = version;

        char[] sep = new char[] { ',' };
        string[] parts = tuple.Split(sep);
        startTime = parts[0];
        sourceAddress = parts[1];
        destinationAddress = parts[2];
        sourcePort = parts[3];
        destinationPort = parts[4];
        transportProtocol = parts[5];
        deviceDirection = parts[6];
        deviceAction = parts[7];

        if (version >= 2.0)
        {
            flowState = parts[8];
            if (flowState != "B")
            {
                packetsStoD = (parts[9] == "" ? "0" : parts[9]);
                bytesStoD = (parts[10] == "" ? "0" : parts[10]);
                packetsDtoS = (parts[11] == "" ? "0" : parts[11]);
                bytesDtoS = (parts[12] == "" ? "0" : parts[12]);
            }
        }
    }

    public string GetDirection
    {
        get { return deviceDirection; }
    }

    public override string ToString()
    {
        var temp = new StringBuilder();
        temp.Append("rt=").Append((Convert.ToUInt64(startTime) * 1000).ToString());
        temp.Append(" src=").Append(sourceAddress);
        temp.Append(" dst=").Append(destinationAddress);
        temp.Append(" spt=").Append(sourcePort);
        temp.Append(" dpt=").Append(destinationPort);
        temp.Append(" proto=").Append((transportProtocol == "U" ? "17" : "6"));
        temp.Append(" deviceDirection=").Append((deviceDirection == "I" ? "0" : "1"));
        temp.Append(" act=").Append(deviceAction);

        if (schemaVersion >= 2.0)
        {
            // add fields from version 2 schema
            temp.Append(" cs2=").Append(flowState);
            temp.Append(" cs2Label=FlowState");

            if (flowState != "B")
            {
                temp.Append(" cn1=").Append(packetsStoD);
                temp.Append(" cn1Label=PacketsStoD");
                temp.Append(" cn2=").Append(packetsDtoS);
                temp.Append(" cn2Label=PacketsDtoS");

                if (deviceDirection == "I")
                {
                    temp.Append(" bytesIn=").Append(bytesStoD);
                    temp.Append(" bytesOut=").Append(bytesDtoS);
                }
                else
                {
                    temp.Append(" bytesIn=").Append(bytesDtoS);
                    temp.Append(" bytesOut=").Append(bytesStoD);
                }
            }
        }

        return temp.ToString();
    }

    public string JsonSubString()
    {
        var sb = new StringBuilder();
        sb.Append(",\"rt\":\"").Append((Convert.ToUInt64(startTime) * 1000).ToString()).Append("\"");
        sb.Append(",\"src\":\"").Append(sourceAddress).Append("\"");
        sb.Append(",\"dst\":\"").Append(destinationAddress).Append("\"");
        sb.Append(",\"spt\":\"").Append(sourcePort).Append("\"");
        sb.Append(",\"dpt\":\"").Append(destinationPort).Append("\"");
        sb.Append(",\"proto\":\"").Append((transportProtocol == "U" ? "UDP" : "TCP")).Append("\"");
        sb.Append(",\"deviceDirection\":\"").Append((deviceDirection == "I" ? "0" : "1")).Append("\"");
        sb.Append(",\"act\":\"").Append(deviceAction).Append("\"");

        return sb.ToString();
    }
}

class NSGFlowLogsInnerFlows
{
    public string mac { get; set; }
    public string[] flowTuples { get; set; }

    public string MakeMAC()
    {
        var temp = new StringBuilder();
        temp.Append(mac.Substring(0, 2)).Append(":");
        temp.Append(mac.Substring(2, 2)).Append(":");
        temp.Append(mac.Substring(4, 2)).Append(":");
        temp.Append(mac.Substring(6, 2)).Append(":");
        temp.Append(mac.Substring(8, 2)).Append(":");
        temp.Append(mac.Substring(10, 2));

        return temp.ToString();
    }
}

class NSGFlowLogsOuterFlows
{
    public string rule { get; set; }
    public NSGFlowLogsInnerFlows[] flows { get; set; }
}

class NSGFlowLogProperties
{
    public float Version { get; set; }
    public NSGFlowLogsOuterFlows[] flows { get; set; }
}

class NSGFlowLogRecord
{
    public string time { get; set; }
    public string systemId { get; set; }
    public string macAddress { get; set; }
    public string category { get; set; }
    public string resourceId { get; set; }
    public string operationName { get; set; }
    public NSGFlowLogProperties properties { get; set; }

    public string MakeDeviceExternalID()
    {
        var patternSubscriptionId = "SUBSCRIPTIONS\\/(.*?)\\/";
        var patternResourceGroup = "SUBSCRIPTIONS\\/(?:.*?)\\/RESOURCEGROUPS\\/(.*?)\\/";
        var patternResourceName = "PROVIDERS\\/(?:.*?\\/.*?\\/)(.*?)(?:\\/|$)";

        Match m = Regex.Match(resourceId, patternSubscriptionId);
        var subscriptionID = m.Groups[1].Value;

        m = Regex.Match(resourceId, patternResourceGroup);
        var resourceGroup = m.Groups[1].Value;

        m = Regex.Match(resourceId, patternResourceName);
        var resourceName = m.Groups[1].Value;

        return subscriptionID + "/" + resourceGroup + "/" + resourceName;
    }

    public string MakeCEFTime()
    {
        // sample input: "2017-08-09T00:13:25.4850000Z"
        // sample output: Aug 09 00:13:25 host CEF:0

        CultureInfo culture = new CultureInfo("en-US");
        DateTime tempDate = Convert.ToDateTime(time, culture);
        string newTime = tempDate.ToString("MMM dd HH:mm:ss");

        return newTime + " host CEF:0";
    }

    public override string ToString()
    {
        string temp = MakeDeviceExternalID();
        return temp;
    }
}

class NSGFlowLogRecords
{
    public NSGFlowLogRecord[] records { get; set; }
}
