const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, ScanCommand } = require('@aws-sdk/lib-dynamodb');

const ddbClient = new DynamoDBClient();
const dynamodb = DynamoDBDocumentClient.from(ddbClient);

exports.handler = async (event) => {
  try {
    const result = await dynamodb.send(new ScanCommand({
      TableName: 'prism-news',
      Limit: 50
    }));

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify(result.Items)
    };
  } catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ error: 'Failed to fetch news' })
    };
  }
}
