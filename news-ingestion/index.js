const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { EventBridgeClient, PutEventsCommand } = require('@aws-sdk/client-eventbridge');
const https = require('https');

const ddbClient = new DynamoDBClient();
const dynamodb = DynamoDBDocumentClient.from(ddbClient);
const eventbridge = new EventBridgeClient();

exports.handler = async (event) => {
  try {
    console.log('Starting news ingestion...');
    const newsApiKey = process.env.NEWS_API_KEY;
    
    if (!newsApiKey) {
      throw new Error('NEWS_API_KEY environment variable is not set');
    }
    
    const url = `https://newsapi.org/v2/top-headlines?country=us&apiKey=${newsApiKey}`;
    console.log('Fetching news from NewsAPI...');
    
    const response = await fetchNews(url);
    console.log('Response received:', JSON.stringify(response).substring(0, 200) + '...');
    
    const articles = response?.articles || [];
    console.log(`Found ${articles.length} articles`);
    
    for (const article of articles) {
      const item = {
        id: `${Date.now()}-${Math.random().toString(36).substring(7)}`,
        timestamp: new Date().toISOString(),
        title: article.title || 'No title',
        content: article.content || article.description || '',
        source: article.source?.name || 'Unknown',
        url: article.url || '',
        ttl: Math.floor(Date.now() / 1000) + (7 * 24 * 60 * 60)
      };

      console.log(`Processing article: ${item.title}`);

      await dynamodb.send(new PutCommand({
        TableName: 'prism-news',
        Item: item
      }));

      await eventbridge.send(new PutEventsCommand({
        Entries: [{
          Source: 'prism.news',
          DetailType: 'news.ingested',
          Detail: JSON.stringify(item)
        }]
      }));
      
      console.log(`Article processed: ${item.id}`);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'News ingestion completed',
        articlesProcessed: articles.length
      })
    };
  } catch (error) {
    console.error('Error:', error.message);
    console.error('Stack:', error.stack);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error.message
      })
    };
  }
};

function fetchNews(url) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Prism-News-Lambda/1.0',  // Added User-Agent header
        'Accept': 'application/json'
      }
    };
    
    const req = https.get(url, options, (res) => {
      let data = '';
      
      console.log('API Status:', res.statusCode);
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        try {
          if (res.statusCode !== 200) {
            console.error('API Error:', data);
            reject(new Error(`NewsAPI returned status ${res.statusCode}`));
            return;
          }
          
          const parsedData = JSON.parse(data);
          if (!parsedData.articles) {
            reject(new Error('Invalid API response: no articles found'));
            return;
          }
          
          resolve(parsedData);
        } catch (e) {
          console.error('Parse Error:', e);
          reject(new Error('Failed to parse API response'));
        }
      });
    });

    req.on('error', (e) => {
      console.error('Request Error:', e);
      reject(new Error('Failed to fetch from NewsAPI'));
    });

    // Set timeout
    req.setTimeout(8000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}
