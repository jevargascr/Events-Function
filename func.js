const fdk = require('@fnproject/fdk');
const os = require("oci-objectstorage")
const common = require("oci-common")
const streaming = require("oci-streaming")
const crypto = require("crypto");


fdk.handle(async function (input) {
  if (!input) {
    return { 'message': 'No event' }
  }
  const provider = common.ResourcePrincipalAuthenticationDetailsProvider.builder();

  let body = input

  const response = await getObjectAndSend(body, provider)


  return response
})


async function getObjectAndSend(eventPayload, provider) {
  console.log("Starting function flow...")

  const objectClient = new os.ObjectStorageClient({
    authenticationDetailsProvider: provider
  })

  const streamClient = new streaming.StreamClient({
    authenticationDetailsProvider: provider
  })

  const streamAdminClient = new streaming.StreamAdminClient({
    authenticationDetailsProvider: provider
  })

  const getObjectRequest = {
    objectName: eventPayload.data.resourceName,
    bucketName: eventPayload.data.additionalDetails.bucketName,
    namespaceName: eventPayload.data.additionalDetails.namespace
  }
  console.log("Getting object information...")
  const getObjectResponse = await objectClient.getObject(getObjectRequest)
  if (!getObjectResponse) {
    console.log("Couldn't fetch object")
    return "Couldn't fetch object"
  }
  console.log("[Object Response] - ", true)
  let fileContent = await readAndSend(getObjectResponse.value, eventPayload.data.compartmentId, streamClient, streamAdminClient)
  console.log("[File Content] - ",fileContent)
  await publishMessage(fileContent, eventPayload.data.compartmentId ,streamClient, streamAdminClient)
  return "[Message Delivered Successfully]"
}


async function readAndSend(file) {
  let fileData = "";
  console.log("Reading file content...")    
  file.on("data", function (data) {
    fileData += data.toString();
  });
  await delay(2);
  return fileData    
}


async function publishMessage(content, compartmentId, streamClient, streamAdminClient) {
    
  let streamConf = await getStream(compartmentId, streamAdminClient)

  const id = crypto.randomBytes(20).toString('hex')

  let message = {
    key: Buffer.from(id).toString("base64"),
    value: Buffer.from(content).toString("base64"),
  }

  let messages = []

  messages.push(message)

  streamClient.endpoint = streamConf.messagesEndpoint

  const putMessageDetails = { messages: messages };
  const putMessagesRequest = {
    putMessagesDetails: putMessageDetails,
    streamId: streamConf.id
  };
  console.log("Sending Message...")
  const putMessageResponse = await streamClient.putMessages(putMessagesRequest);
  await delay(1);
  console.log("[Request ID] - ",putMessageResponse.opcRequestId)
}


async function getStream(compartmentId,streamAdminClient) {
  console.log("Getting Stream Information...")
  const listStreamsRequest = {
    compartmentId: compartmentId,
    name: 'workshop',
    lifecycleState: streaming.models.Stream.LifecycleState.Active.toString()
  };
  const listStreamsResponse = await streamAdminClient.listStreams(listStreamsRequest);
  const stream = listStreamsResponse.items[0]
  console.log("[Stream ID] - ",stream.id)
  return stream
}

async function delay(s) {
  return new Promise(resolve => setTimeout(resolve, s * 1000));
}
